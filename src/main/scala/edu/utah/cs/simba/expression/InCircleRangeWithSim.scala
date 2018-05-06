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

package edu.utah.cs.simba.expression

import edu.utah.cs.simba.{ShapeSerializer, ShapeType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.types.{NumericType, StringType}
import edu.utah.cs.simba.spatial.{Circle, Point, Shape}
import edu.utah.cs.simba.util.{NumberUtil, ShapeUtils}
import org.apache.spark.sql.catalyst.util.GenericArrayData


case class InCircleRangeWithSim(shape: Expression, target: Expression, radius: Literal, lText: Expression,
                                rText: Expression, sim: Literal)
  extends Predicate with CodegenFallback {
  require(radius.dataType.isInstanceOf[NumericType])
  require(sim.dataType.isInstanceOf[NumericType])

  override def children: Seq[Expression] = Seq(shape, target, radius, lText, rText, sim)

  override def nullable: Boolean = false

  override def toString: String = s" **($shape) IN CIRCLERANGE ($target) within  ($radius) and textual similarity larger than ($sim)** "

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = {
    val eval_shape = ShapeUtils.getShape(shape, input)
    val eval_target = target.eval(input).asInstanceOf[Point]
    require(eval_shape.dimensions == eval_target.dimensions)
    val eval_r = NumberUtil.literalToDouble(radius)
    Circle(eval_target, eval_r).intersects(eval_shape)
  }
}
