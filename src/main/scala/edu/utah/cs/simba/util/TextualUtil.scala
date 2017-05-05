package edu.utah.cs.simba.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.catalyst.util.ArrayData

import scala.collection.mutable

/**
  * Created by zdc on 2017/5/3.
  */
object TextualUtil {
  def simFilter(leftText: ArrayData, rightText:ArrayData, sim: Double): Boolean = {
    val l = leftText.numElements()
    val r = rightText.numElements()
    if(sim * l > r || sim * r > l ) return false
    var sameText = 0
    val data = mutable.Set[String]()
    var i = 0
    while (i < l) {
      data.add(leftText.getUTF8String(i).toString)
      i += 1
    }
    var j = 0
    while (j < r){
      val tmp_str = rightText.getUTF8String(j).toString
      if(data.contains(tmp_str)) sameText += 1
      else data.add(tmp_str)
      j += 1
    }
    if(sameText/1.0/data.size >= sim) return true
    false
  }

  def getText(expression: Expression, schema: Seq[Attribute], input: InternalRow): ArrayData = {
    BindReferences.bindReference(expression, schema).eval(input).asInstanceOf[ArrayData]
  }
}
