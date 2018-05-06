package edu.utah.cs.simba.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

/**
  * Created by zdc on 2017/5/3.
  */
object TextualUtil {
  /*
    Calculate jaccard similarity between left and right set and judge if it's bigger than threshold
   */
  def simFilter(leftText: ArrayData, rightText:ArrayData, sim: Double): Boolean = {
    val l = leftText.numElements()
    val r = rightText.numElements()
    if(sim * l > r || sim * r > l ) return false
    var sameNum = 0
    val setLeft = mutable.Set[String]()
    val setRight = mutable.Set[String]()
    var i = 0
    while (i < l) {
      setLeft.add(leftText.getUTF8String(i).toString)
      i += 1
    }
    var j = 0
    while (j < r){
      val tmpStr = rightText.getUTF8String(j).toString
      if(setLeft.contains(tmpStr)) sameNum += 1
      setRight.add(tmpStr)
      j += 1
    }
    if((sameNum * 1.0 / (setLeft.size + setRight.size - sameNum)) >= sim) return true
    false
  }
  /*
    Calculate jaccard similarity between left and right set and judge if it's bigger than threshold
   */
  def simFilter(leftText: Array[String], rightText: Array[String], sim: Double): Boolean = {
    val l = leftText.length
    val r = rightText.length
    if(sim * l > r || sim * r > l ) return false
    var sameNum = 0
    val setLeft = mutable.Set[String]()
    val setRight = mutable.Set[String]()
    var i = 0
    while (i < l) {
      setLeft.add(leftText(i))
      i += 1
    }
    var j = 0
    while (j < r){
      val tmpStr = rightText(j)
      if(setLeft.contains(tmpStr)) sameNum += 1
      setRight.add(tmpStr)
      j += 1
    }
    if((sameNum * 1.0 / (setLeft.size + setRight.size - sameNum)) >= sim) return true
    false
  }

  def getText(expression: Expression, schema: Seq[Attribute], input: InternalRow): ArrayData = {
    BindReferences.bindReference(expression, schema).eval(input).asInstanceOf[ArrayData]
  }
  def getTextAsStrings(expression: Expression, schema: Seq[Attribute], input: InternalRow): Array[String] = {
    BindReferences.bindReference(expression, schema).eval(input).asInstanceOf[ArrayData]
      .toArray[UTF8String](StringType).map(x => x.toString)
  }
}
