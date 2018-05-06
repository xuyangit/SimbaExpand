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
import edu.utah.cs.simba.index.RTreeType
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by dongx on 11/14/2016.
  */
object TestMain {
  case class PointData(x: Double, y: Double, str_set: Array[String], other: String)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("STJoin").setMaster("local[4]")
    /*val joinMethods = Array("STJSparkPFTree", "AllPairsSTJSparkPFTree", "STJSpark1R",
                            "STJSpark2R", "AllPairsSTJSpark2R",
                            "AllPairsSTJSpark1R")*/
    val joinMethods = Array("STJSparkPFTree", "AllPairsSTJSparkPFTree",
      "AllPairsSTJSpark1R")
    //val joinMethods = Array("STJSpark2R")
    var leftData = ListBuffer[PointData]()
    var rightData = ListBuffer[PointData]()

    val file = Source.fromFile("D:\\STJoin\\Dataset\\Osm\\osm_text_100000")
    val minMax = Array[Double](10000, 10000, -1, -1)
    for(line <- file.getLines) {
      val info = line.split(" ")
      minMax(0) = Math.min(minMax(0), info(1).toDouble)
      minMax(1) = Math.min(minMax(1), info(2).toDouble)
      minMax(2) = Math.max(minMax(2), info(1).toDouble)
      minMax(3) = Math.max(minMax(3), info(2).toDouble)
      leftData += PointData(info(1).toDouble, info(2).toDouble, info(3).split("\\?").sorted, "a=" + info(0))
    }
    minMax.foreach(println)
    rightData = leftData

    joinMethods.map{ method =>
      val sc = new SparkContext(sparkConf)
      // set log level for debug
      sc.setLogLevel("OFF")
      val simbaContext = new SimbaContext(sc)
      simbaContext.simbaConf.setConfString("simba.join.stjoin", method)
      simbaContext.simbaConf.setConfString("simba.join.partitions", "1")

      import simbaContext.implicits._
      import simbaContext.SimbaImplicits._
      val leftDF = sc.parallelize(leftData).toDF
      val rightDF = sc.parallelize(rightData).toDF

      leftDF.registerTempTable("point1")
      val startTime = System.currentTimeMillis()
      println(leftDF.stJoin(rightDF, Array("x", "y"), Array("x", "y"), 16, "str_set", "str_set", 0.9).count())
      val endTime = System.currentTimeMillis()
      sc.stop()
      method + " with time cost: " + ((endTime - startTime) / 1000.0) + "s"
    }.foreach(println)
  }
}
