/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package edu.utah.cs.simba.execution.join

import edu.utah.cs.simba.execution.SimbaPlan
import edu.utah.cs.simba.index._
import edu.utah.cs.simba.partitioner.{MapDPartition, STRPartition}
import edu.utah.cs.simba.spatial.Point
import edu.utah.cs.simba.util.{NumberUtil, ShapeUtils, TextualUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

/*
  Spatial-textual join implementation based on DJSpark, which generate all pairs of original
  left and right partitions which can join together. We further use PPJoin-first, Two R-Tree
  Join and One Prefix-filter based R-Tree based on DJSpark. This corresponds with STJSpark2R
  and PSTJSpark.
 */
case class AllPairsSTJSpark2R(left_key: Expression, right_key: Expression, l: Literal, lTextKey: Expression, rTextKey: Expression,
                     s: Literal, left: SparkPlan, right: SparkPlan) extends SimbaPlan {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = simbaContext.simbaConf.joinPartitions
  final val sample_rate = simbaContext.simbaConf.sampleRate
  final val max_entries_per_node = simbaContext.simbaConf.maxEntriesPerNode
  final val transfer_threshold = simbaContext.simbaConf.transferThreshold
  final val r = NumberUtil.literalToDouble(l)
  final val sim = NumberUtil.literalToDouble(s)

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (ShapeUtils.getShape(left_key, left.output, row).asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (ShapeUtils.getShape(right_key, right.output, row).asInstanceOf[Point], row)
    )

    val dimension = right_rdd.first()._1.coord.length

    val (left_partitioned, left_mbr_bound) = STRPartition(left_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)
    val (right_partitioned, right_mbr_bound) = STRPartition(right_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)

    val right_rt = RTree(right_mbr_bound.zip(Array.fill[Int](right_mbr_bound.length)(0))
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)

    val left_dup = new Array[Array[Int]](left_mbr_bound.length)
    val right_dup = new Array[Array[Int]](right_mbr_bound.length)

    var tot = 0
    left_mbr_bound.foreach { now =>
      val res = right_rt.circleRange(now._1, r)
      val tmp_arr = mutable.ArrayBuffer[Int]()
      res.foreach {x =>
        if (right_dup(x._2) == null) right_dup(x._2) = Array(tot)
        else right_dup(x._2) = right_dup(x._2) :+ tot
        tmp_arr += tot
        tot += 1
      }
      left_dup(now._2) = tmp_arr.toArray
    }

    val bc_left_dup = sparkContext.broadcast(left_dup)
    val bc_right_dup = sparkContext.broadcast(right_dup)

    val left_dup_rdd = left_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_left_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val right_dup_rdd = right_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_right_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val left_dup_partitioned = MapDPartition(left_dup_rdd, tot).map(_._2)
    val right_dup_partitioned = MapDPartition(right_dup_rdd, tot).map(_._2)

    left_dup_partitioned.zipPartitions(right_dup_partitioned) {(leftIter, rightIter) =>
      val ans = mutable.ListBuffer[InternalRow]()
      val leftData = leftIter.toArray
      val right_data = rightIter.toArray
      if (leftData.length > 0 && right_data.length > 0) {
        val left_index = RTree(leftData.map(_._1).zipWithIndex, max_entries_per_node)
        val right_index = AugmentRTree(
          right_data.zipWithIndex.map(x =>
            (x._1._1, x._2, TextualUtil.getTextAsStrings(rTextKey, right.output, x._1._2))),
          max_entries_per_node,
          sim)
        doJoin(left_index.root, right_index.root, r, sim, ans, leftData, right_data)
      }
      ans.iterator
    }
  }

  def doJoin(left: RTreeNode, right: ARNode, dis:Double, sim: Double, result: mutable.ListBuffer[InternalRow],
             l_data: Array[(Point,InternalRow)], r_data: Array[(Point,InternalRow)]):Unit = {
    if(left.isLeaf && right.isLeaf) {
      result ++= PPJoin(left, right, dis, sim, l_data, r_data)
      return
    }
    //
    val e_left = left.m_mbr.expand(dis)
    val e_right = right.m_mbr.expand(dis)
    if(e_left.intersects(right.m_mbr)){
      val overlap = e_left.overLap(e_right)
      var left_entry = Array[RTreeNode]()
      var right_entry = Array[ARNode]()
      if(!left.isLeaf) left_entry ++= left.m_child.filter(x => x.intersects(overlap)).map(x => x.asInstanceOf[RTreeInternalEntry].node)
      else left_entry ++= Array(left)
      if(!right.isLeaf) right_entry ++= right.m_child.filter(x => x.intersects(overlap)).map(x => x.asInstanceOf[ARInternalEntry].node)
      else right_entry ++= Array(right)
      left_entry.foreach(x => {
        right_entry.foreach(y => {
          if(x.m_mbr.expand(dis).intersects(y.m_mbr)) doJoin(x,y,dis,sim,result,l_data,r_data)
        })
      })
    }
    else return
  }

  def PPJoin(leftNode: RTreeNode, rightNode: ARNode, dis: Double, sim: Double, l_data: Array[(Point,InternalRow)],
             r_data: Array[(Point,InternalRow)]): mutable.ListBuffer[InternalRow] = {
    val ans = mutable.ListBuffer[InternalRow]()
    var leftData = mutable.ListBuffer[(Point, Array[String], InternalRow)]()
    leftData ++= leftNode.m_child.map(x => {
      val info = l_data(x.asInstanceOf[RTreeLeafEntry].m_data)
      (info._1, TextualUtil.getTextAsStrings(lTextKey, left.output, info._2), info._2)
    })
    val inverted = rightNode.index
    leftData.foreach(x => {
      val p = x._2.length - Math.ceil(sim * x._2.length).asInstanceOf[Int] + 1
      val candidates = mutable.LinkedHashSet[ARLeafEntry]()
      for (i <- 0 to p - 1) {
        val str = x._2(i)
        if(inverted.contains(str)) {
          inverted.get(str).get.filter(y => {
            val ll = y.strings.length
            val rl = x._2.length
            sim * ll <= rl && sim * rl <= ll && y.shape.minDist(x._1) <= dis
          }).foreach(k => {
            candidates.add(k)
          })
        }
      }
      ans ++= candidates.filter(c =>
        TextualUtil.simFilter(c.strings, x._2, sim))
        .map(row => new JoinedRow(x._3, r_data(row.m_data)._2))
    })
    ans
  }

  override def children: Seq[SparkPlan] = Seq(left, right)
}

case class AllPairsSTJSpark1R(left_key: Expression, right_key: Expression, l: Literal, lTextKey: Expression, rTextKey: Expression,
                               s: Literal, left: SparkPlan, right: SparkPlan) extends SimbaPlan {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = simbaContext.simbaConf.joinPartitions
  final val sample_rate = simbaContext.simbaConf.sampleRate
  final val max_entries_per_node = simbaContext.simbaConf.maxEntriesPerNode
  final val transfer_threshold = simbaContext.simbaConf.transferThreshold
  final val r = NumberUtil.literalToDouble(l)
  final val sim = NumberUtil.literalToDouble(s)

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (ShapeUtils.getShape(left_key, left.output, row).asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (ShapeUtils.getShape(right_key, right.output, row).asInstanceOf[Point], row)
    )

    val dimension = right_rdd.first()._1.coord.length

    val (left_partitioned, left_mbr_bound) = STRPartition(left_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)
    val (right_partitioned, right_mbr_bound) = STRPartition(right_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)

    val right_rt = RTree(right_mbr_bound.zip(Array.fill[Int](right_mbr_bound.length)(0))
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)

    val left_dup = new Array[Array[Int]](left_mbr_bound.length)
    val right_dup = new Array[Array[Int]](right_mbr_bound.length)

    var tot = 0
    left_mbr_bound.foreach { now =>
      val res = right_rt.circleRange(now._1, r)
      val tmp_arr = mutable.ArrayBuffer[Int]()
      res.foreach {x =>
        if (right_dup(x._2) == null) right_dup(x._2) = Array(tot)
        else right_dup(x._2) = right_dup(x._2) :+ tot
        tmp_arr += tot
        tot += 1
      }
      left_dup(now._2) = tmp_arr.toArray
    }

    val bc_left_dup = sparkContext.broadcast(left_dup)
    val bc_right_dup = sparkContext.broadcast(right_dup)

    val left_dup_rdd = left_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_left_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val right_dup_rdd = right_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_right_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val left_dup_partitioned = MapDPartition(left_dup_rdd, tot).map(_._2)
    val right_dup_partitioned = MapDPartition(right_dup_rdd, tot).map(_._2)

    left_dup_partitioned.zipPartitions(right_dup_partitioned) {(leftIter, rightIter) =>
      val ans = mutable.ListBuffer[InternalRow]()
      val right_data = rightIter.toArray
      if (right_data.length > 0) {
        val right_index = AugmentRTree(
          right_data.zipWithIndex.map(x =>
            (x._1._1, x._2, TextualUtil.getTextAsStrings(rTextKey, right.output, x._1._2))),
          max_entries_per_node,
          sim)
        leftIter.foreach {now =>
          val leftText = TextualUtil.getTextAsStrings(lTextKey, left.output, now._2)
          ans ++= right_index.stSimilar(now._1, r, leftText, sim)
            .map(x => new JoinedRow(now._2, right_data(x)._2))
        }
      }
      ans.iterator
    }
  }

  override def children: Seq[SparkPlan] = Seq(left, right)
}

case class AllPairsSTJSparkPFTree(left_key: Expression, right_key: Expression, l: Literal, lTextKey: Expression, rTextKey: Expression,
                              s: Literal, left: SparkPlan, right: SparkPlan) extends SimbaPlan {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = simbaContext.simbaConf.joinPartitions
  final val sample_rate = simbaContext.simbaConf.sampleRate
  final val false_rate = simbaContext.simbaConf.falseRate
  final val max_entries_per_node = simbaContext.simbaConf.maxEntriesPerNode
  final val transfer_threshold = simbaContext.simbaConf.transferThreshold
  final val r = NumberUtil.literalToDouble(l)
  final val sim = NumberUtil.literalToDouble(s)

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (ShapeUtils.getShape(left_key, left.output, row).asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (ShapeUtils.getShape(right_key, right.output, row).asInstanceOf[Point], row)
    )

    val dimension = right_rdd.first()._1.coord.length

    val (left_partitioned, left_mbr_bound) = STRPartition(left_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)
    val (right_partitioned, right_mbr_bound) = STRPartition(right_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)

    val right_rt = RTree(right_mbr_bound.zip(Array.fill[Int](right_mbr_bound.length)(0))
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)

    val left_dup = new Array[Array[Int]](left_mbr_bound.length)
    val right_dup = new Array[Array[Int]](right_mbr_bound.length)

    var tot = 0
    left_mbr_bound.foreach { now =>
      val res = right_rt.circleRange(now._1, r)
      val tmp_arr = mutable.ArrayBuffer[Int]()
      res.foreach {x =>
        if (right_dup(x._2) == null) right_dup(x._2) = Array(tot)
        else right_dup(x._2) = right_dup(x._2) :+ tot
        tmp_arr += tot
        tot += 1
      }
      left_dup(now._2) = tmp_arr.toArray
    }

    val bc_left_dup = sparkContext.broadcast(left_dup)
    val bc_right_dup = sparkContext.broadcast(right_dup)

    val left_dup_rdd = left_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_left_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val right_dup_rdd = right_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_right_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val left_dup_partitioned = MapDPartition(left_dup_rdd, tot).map(_._2)
    val right_dup_partitioned = MapDPartition(right_dup_rdd, tot).map(_._2)

    left_dup_partitioned.zipPartitions(right_dup_partitioned) {(leftIter, rightIter) =>
      val ans = mutable.ListBuffer[InternalRow]()
      val right_data = rightIter.toArray
      if (right_data.length > 0) {
        val right_index = PrefixFilterTree(
          right_data.zipWithIndex.map(x =>
            (x._1._1, x._2, TextualUtil.getTextAsStrings(rTextKey, right.output, x._1._2))),
          max_entries_per_node,
          sim,
          false_rate)
        leftIter.foreach {now =>
          val leftText = TextualUtil.getTextAsStrings(lTextKey, left.output, now._2)
          ans ++= right_index.stSimilar(now._1, r, leftText, sim)
            .map(x => new JoinedRow(now._2, right_data(x)._2))
        }
      }
      ans.iterator
    }
  }

  override def children: Seq[SparkPlan] = Seq(left, right)
}

case class AllPairsPSTJSpark(left_key: Expression, right_key: Expression, l: Literal, lTextKey: Expression, rTextKey: Expression,
                              s: Literal, left: SparkPlan, right: SparkPlan) extends SimbaPlan {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = simbaContext.simbaConf.joinPartitions
  final val sample_rate = simbaContext.simbaConf.sampleRate
  final val max_entries_per_node = simbaContext.simbaConf.maxEntriesPerNode
  final val transfer_threshold = simbaContext.simbaConf.transferThreshold
  final val r = NumberUtil.literalToDouble(l)
  final val sim = NumberUtil.literalToDouble(s)

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (ShapeUtils.getShape(left_key, left.output, row).asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (ShapeUtils.getShape(right_key, right.output, row).asInstanceOf[Point], row)
    )

    val dimension = right_rdd.first()._1.coord.length

    val (left_partitioned, left_mbr_bound) = STRPartition(left_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)
    val (right_partitioned, right_mbr_bound) = STRPartition(right_rdd, dimension, num_partitions,
      sample_rate, transfer_threshold, max_entries_per_node)

    val right_rt = RTree(right_mbr_bound.zip(Array.fill[Int](right_mbr_bound.length)(0))
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)

    val left_dup = new Array[Array[Int]](left_mbr_bound.length)
    val right_dup = new Array[Array[Int]](right_mbr_bound.length)

    var tot = 0
    left_mbr_bound.foreach { now =>
      val res = right_rt.circleRange(now._1, r)
      val tmp_arr = mutable.ArrayBuffer[Int]()
      res.foreach {x =>
        if (right_dup(x._2) == null) right_dup(x._2) = Array(tot)
        else right_dup(x._2) = right_dup(x._2) :+ tot
        tmp_arr += tot
        tot += 1
      }
      left_dup(now._2) = tmp_arr.toArray
    }

    val bc_left_dup = sparkContext.broadcast(left_dup)
    val bc_right_dup = sparkContext.broadcast(right_dup)

    val left_dup_rdd = left_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_left_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val right_dup_rdd = right_partitioned.mapPartitionsWithIndex { (id, iter) =>
      iter.flatMap {now =>
        val tmp_list = bc_right_dup.value(id)
        if (tmp_list != null) tmp_list.map(x => (x, now))
        else Array[(Int, (Point, InternalRow))]()
      }
    }

    val left_dup_partitioned = MapDPartition(left_dup_rdd, tot).map(_._2)
    val right_dup_partitioned = MapDPartition(right_dup_rdd, tot).map(_._2)

    left_dup_partitioned.zipPartitions(right_dup_partitioned) {(leftIter, rightIter) =>
      val ans = mutable.ListBuffer[InternalRow]()
      val left_info = leftIter.toArray
      val right_info = rightIter.toArray
      var leftData = mutable.ListBuffer[(Point, ArrayData, InternalRow)]()
      var right_data = mutable.ListBuffer[(Point, ArrayData, InternalRow)]()
      leftData ++= left_info.map(x =>
        (x._1, TextualUtil.getText(lTextKey, left.output, x._2), x._2)
      )
      right_data ++= right_info.map(x =>
        (x._1, TextualUtil.getText(rTextKey, right.output, x._2), x._2)
      )
      //leftData.sortWith((x, y) => x._2.numElements() < y._2.numElements())
      //right_data.sortWith((x, y) => x._2.numElements() < y._2.numElements())
      val inverted = mutable.HashMap[String, mutable.ListBuffer[(Point, ArrayData, InternalRow)]]()
      right_data.foreach(x => {
        val p = x._2.numElements() - Math.ceil(sim * x._2.numElements()).asInstanceOf[Int] + 1
        for (i <- 0 to p - 1) {
          val str = x._2.getUTF8String(i).toString
          if (inverted.contains(str)) inverted.get(str).get.append(x)
          else inverted.put(str, mutable.ListBuffer[(Point, ArrayData, InternalRow)](x))
        }
      })
      leftData.foreach(x => {
        val p = x._2.numElements() - Math.ceil(sim * x._2.numElements()).asInstanceOf[Int] + 1
        val compare_set = mutable.LinkedHashSet[(Point, ArrayData, InternalRow)]()
        for (i <- 0 to p - 1) {
          val str = x._2.getUTF8String(i).toString
          if(inverted.contains(str)) {
            inverted.get(str).get.filter(y => {
              val ll = y._2.numElements()
              val rl = x._2.numElements()
              sim * ll <= rl && sim * rl <= ll && y._1.minDist(x._1) <= r
            }).foreach(k => {
              compare_set.add(k)
            })
          }
        }
        ans ++= compare_set.filter(c =>
          TextualUtil.simFilter(c._2, x._2, sim)).map(row => new JoinedRow(x._3, row._3))
      })
      ans.iterator
    }
  }

  override def children: Seq[SparkPlan] = Seq(left, right)
}