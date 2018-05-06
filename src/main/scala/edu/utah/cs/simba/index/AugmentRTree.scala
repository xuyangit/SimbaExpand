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

package edu.utah.cs.simba.index

import edu.utah.cs.simba.spatial._
import edu.utah.cs.simba.util.{BloomFilter, TextualUtil}

import scala.collection.mutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

case class ARLeafEntry(shape: Shape, strings: Array[String], m_data: Int, size: Int) extends RTreeEntry {
  override def minDist(x: Shape): Double = shape.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(shape)
}
case class ARInternalEntry(mbr: MBR, node: ARNode) extends RTreeEntry {
  override def minDist(x: Shape): Double = mbr.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(mbr)
}
case class ARNode(m_mbr: MBR, m_child: Array[RTreeEntry], isLeaf: Boolean,
                  index: HashMap[String, ListBuffer[ARLeafEntry]]) {
  val size: Long = {
    if (isLeaf) m_child.map(x => x.asInstanceOf[ARLeafEntry].size).sum
    else m_child.map(x => x.asInstanceOf[ARInternalEntry].node.size).sum
  }
}

object ARNode {
  def fromNodes(m_mbr: MBR, children: Array[(MBR, ARNode)]): ARNode = {
    new ARNode(m_mbr, children.map(x => ARInternalEntry(x._1, x._2)), false, null)
  }

  def fromPoints(m_mbr: MBR, children: => Array[(Point, Int, Array[String])], sim: Double): ARNode = {
    var ans = ListBuffer[ARLeafEntry]()
    val points = children
    val invertedList = new HashMap[String, ListBuffer[ARLeafEntry]]()
    for (i <- 0 to points.length - 1) {
      val entry = new ARLeafEntry(points(i)._1, points(i)._3, points(i)._2, 1)
      ans += entry
      //construct the inverted list
      val stringNums = points(i)._3.length - Math.ceil(sim * points(i)._3.length).asInstanceOf[Int] + 1
      for (j <- 0 to stringNums - 1) {
        val postList = invertedList.get(points(i)._3(j))
        if(postList == null || postList.size == 0) {
          invertedList.put(points(i)._3(j), ListBuffer[ARLeafEntry](entry))
        }
        else {
          postList.get.append(entry)
        }
      }
    }
    new ARNode(m_mbr, ans.toArray, true, invertedList)
  }
}
case class AugmentRTree(root: ARNode) extends Index with Serializable{
  def stSimilar(origin: Shape, r: Double,
                keywords: Array[String], sim: Double): Array[Int] = {
    val ans = mutable.ArrayBuffer[Int]()
    val st = new mutable.Stack[ARNode]()
    val len = keywords.length - Math.ceil(sim * keywords.length).asInstanceOf[Int] + 1
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      val stringIndex = now.index
      if (!now.isLeaf) {
        now.m_child.foreach {
          case ARInternalEntry(mbr, node) =>
            if (origin.minDist(mbr) <= r) st.push(node)
        }
      } else {
        val inverted = stringIndex
        val candidates = mutable.LinkedHashSet[ARLeafEntry]()
        for(i <- 0 to len - 1) {
          if(inverted.contains(keywords(i))) {
            inverted.get(keywords(i)).get.filter(y => {
              val ll = y.strings.length
              val rl = keywords.length
              sim * ll <= rl && sim * rl <= ll && y.shape.minDist(origin) <= r
            }).foreach(k => {
              candidates.add(k)
            })
          }
        }
        ans ++= candidates.filter(c =>
          TextualUtil.simFilter(c.strings, keywords, sim)).map(_.m_data)
      }
    }
    ans.toArray
  }
}

object AugmentRTree {

  def apply(entries : Array[(Point, Int, Array[String])], max_entries_per_node: Int,
            sim: Double) : AugmentRTree = {
    val dimension = entries(0)._1.dimensions
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 to dimension - 1) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint(entries: Array[(Point, Int, Array[String])],
                            cur_dim : Int, until_dim : Int): Array[Array[(Point, Int, Array[String])]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_._1.coord(cur_dim) < _._1.coord(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupPoint(now, cur_dim + 1, until_dim))
      } else grouped
    }

    val grouped = recursiveGroupPoint(entries, 0, dimension - 1)
    val pftreeNodes = ListBuffer[(MBR, ARNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(x => Double.MaxValue)
      val max = new Array[Double](dimension).map(x => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.coord(i))
        for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.coord(i))
      })
      val mbr = new MBR(new Point(min), new Point(max))
      pftreeNodes += ((mbr, ARNode.fromPoints(mbr, list, sim)))
    })

    var cur_rtree_nodes = pftreeNodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 to dimension - 1) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def over(dim : Array[Int]) : Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left : (MBR, ARNode), right : (MBR, ARNode)) : Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupPFTreeNode(entries: Array[(MBR, ARNode)], cur_dim : Int, until_dim : Int) : Array[Array[(MBR, ARNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.map(now => {
          recursiveGroupPFTreeNode(now, cur_dim + 1, until_dim)
        }).flatMap(list => list)
      } else grouped
    }

    while (!over(dim)) {
      val grouped = recursiveGroupPFTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = ListBuffer[(MBR, ARNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(x => Double.MaxValue)
        val max = new Array[Double](dimension).map(x => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = new MBR(new Point(min), new Point(max))
        tmp_nodes += ((mbr, ARNode.fromNodes(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 to dimension - 1) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(x => Double.MaxValue)
    val max = new Array[Double](dimension).map(x => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
      for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
    })

    val mbr = new MBR(new Point(min), new Point(max))
    val root = ARNode.fromNodes(mbr, cur_rtree_nodes)
    new AugmentRTree(root)
  }
}
