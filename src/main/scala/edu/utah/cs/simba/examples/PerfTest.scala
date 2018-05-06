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
 */

package edu.utah.cs.simba.examples

import edu.utah.cs.simba.SimbaContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object PerfTest {
  case class PointData(x: Double, y: Double, str_set: Array[String])
  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val sparkConf = new SparkConf().setAppName("STJoin").setMaster("local[4]")
    val joinMethods = Array("STJSparkPFTree", "AllPairsSTJSparkPFTree", "STJSpark1R",
                            "STJSpark2R", "AllPairsSTJSpark2R",
                            "AllPairsSTJSpark1R")
    val dists = Array(1, 5, 10, 15, 20)
    val sims = Array(0.5, 0.6, 0.7, 0.8, 0.9)
    var leftData = ListBuffer[PointData]()
    var rightData = ListBuffer[PointData]()

    joinMethods.foreach { method =>
      dists.foreach { dist =>
        sims.foreach { sim =>
          val sc = new SparkContext(sparkConf)
          // set log level for debug
          sc.setLogLevel("OFF")
          val simbaContext = new SimbaContext(sc)
          simbaContext.simbaConf.setConfString("simba.join.stjoin", method)
          simbaContext.simbaConf.setConfString("simba.join.partitions", "50")
          import simbaContext.implicits._
          import simbaContext.SimbaImplicits._
          val leftDF = sc.textFile(inputFile).map { line =>
            val info = line.split(" ")
            PointData(info(1).toDouble, info(2).toDouble, info(3).split("\\?").sorted)
          }.toDF()
          val rightDF = sc.textFile(inputFile).map { line =>
            val info = line.split(" ")
            PointData(info(1).toDouble, info(2).toDouble, info(3).split("\\?").sorted)
          }.toDF()
          leftDF.registerTempTable("point1")
          val startTime = System.currentTimeMillis()
          leftDF.stJoin(rightDF, Array("x", "y"), Array("x", "y"), dist, "str_set", "str_set", sim).foreach { _ => }
          val endTime = System.currentTimeMillis()
          sc.stop()
          println(method + " " + dist + " " + sim + " " + ((endTime - startTime) / 1000.0))
        }
      }
    }
  }
}
