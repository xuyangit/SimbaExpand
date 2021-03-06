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
import edu.utah.cs.simba.index.RTree
import edu.utah.cs.simba.partitioner.{MapDPartition, STRPartition}
import edu.utah.cs.simba.spatial.Point
import edu.utah.cs.simba.util.{NumberUtil, ShapeUtils, TextualUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

/*
  Spatial-textual join based on Prefix filter of textual information. We first use textual matching then verify
  the pairs. In this class, we also repartition right table so that only i-th partition of left and right can join
  together.
 */
case class PSTJSpark(left_key: Expression, right_key: Expression, l: Literal, lTextKey: Expression, rTextKey: Expression,
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

    val (left_partitioned, left_mbr_bound) =
      STRPartition(left_rdd, dimension, num_partitions, sample_rate,
        transfer_threshold, max_entries_per_node)

    val left_part_size = left_partitioned.mapPartitions {
      iter => Array(iter.length).iterator
    }.collect()

    val left_rt = RTree(left_mbr_bound.zip(left_part_size).map(x => (x._1._1, x._1._2, x._2)),
      max_entries_per_node)
    val bc_rt = sparkContext.broadcast(left_rt)

    val right_dup = right_rdd.flatMap {x =>
      bc_rt.value.circleRange(x._1, r).map(now => (now._2, x))
    }

    val right_dup_partitioned = MapDPartition(right_dup, left_mbr_bound.length)

    left_partitioned.zipPartitions(right_dup_partitioned) {(leftIter, rightIter) =>
      val ans = mutable.ListBuffer[InternalRow]()
      val left_info = leftIter.toArray
      val right_info = rightIter.map(_._2).toArray
      var left_data = mutable.ListBuffer[(Point, ArrayData, InternalRow)]()
      var right_data = mutable.ListBuffer[(Point, ArrayData, InternalRow)]()
      left_data ++= left_info.map(x =>
        (x._1, TextualUtil.getText(lTextKey, left.output, x._2), x._2)
      )
      right_data ++= right_info.map(x =>
        (x._1, TextualUtil.getText(rTextKey, right.output, x._2), x._2)
      )
      //left_data.sortWith((x, y) => x._2.numElements() < y._2.numElements())
      //right_data.sortWith((x, y) => x._2.numElements() < y._2.numElements())
      val invert_file = mutable.HashMap[String, mutable.ListBuffer[(Point, ArrayData, InternalRow)]]()
      right_data.foreach(x => {
        val p = x._2.numElements() - Math.ceil(sim * x._2.numElements()).asInstanceOf[Int] + 1
        for (i <- 0 to p - 1) {
          val str = x._2.getUTF8String(i).toString
          if (invert_file.contains(str)) invert_file.get(str).get.append(x)
          else invert_file.put(str, mutable.ListBuffer[(Point, ArrayData, InternalRow)](x))
        }
      })
      left_data.foreach(x => {
        val p = x._2.numElements() - Math.ceil(sim * x._2.numElements()).asInstanceOf[Int] + 1
        val compare_set = mutable.LinkedHashSet[(Point, ArrayData, InternalRow)]()
        for (i <- 0 to p - 1) {
          val str = x._2.getUTF8String(i).toString
          if(invert_file.contains(str)) {
            invert_file.get(str).get.filter(y => {
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

