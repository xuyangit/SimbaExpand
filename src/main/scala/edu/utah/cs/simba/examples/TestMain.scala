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
    val sparkConf = new SparkConf().setAppName("SpatialOperationExample").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val simbaContext = new SimbaContext(sc)

    var leftData = ListBuffer[PointData]()
    var rightData = ListBuffer[PointData]()

    import simbaContext.implicits._
    import simbaContext.SimbaImplicits._

    val file = Source.fromFile("C:\\Osm_Dataset\\osm_text_10000")
    for(line <- file.getLines) {
      val info = line.split(" ")
      leftData += PointData(info(1).toDouble, info(2).toDouble, info(3).split("\\?").sorted, "a=" + info(0))
    }
    rightData = leftData

    val leftDF = sc.parallelize(leftData).toDF
    val rightDF = sc.parallelize(rightData).toDF

    leftDF.registerTempTable("point1")

//    leftDF.index(RTreeType, "rt", Array("x", "y"))
//    rightDF.index(RTreeType, "rt", Array("x", "y"))

    //simbaContext.sql("SELECT * FROM point1 WHERE x < 10").collect().foreach(println)

    //simbaContext.indexTable("point1", RTreeType, "rt", List("x", "y"))

//    val df = leftDF.knn(Array("x", "y"), Array(10.0, 10), 3)
//    println(df.queryExecution)
//    df.show()

//    leftDF.range(Array("x", "y"), Array(4.0, 5.0), Array(111.0, 222.0)).show(100)

    print(leftDF.stJoin(rightDF, Array("x", "y"), Array("x", "y"), 1, "str_set", 0.4).count())
//
    sc.stop()

  }
}
