/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.bytecode

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, CreateNamedStructUnsafe, Expression, LeafExpression, Literal, NonSQLExpression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, ObjectType, StructType}

/**
 * An expression that wraps another expression and throws a [[NullPointerException]] if the
 * child expression evaluates to null.
 *
 * This expression is needed as null handling in SQL is semantically different from Java/Scala.
 *
 * @param child the child expression.
 */
case class NPEonNull(child: Expression) extends UnaryExpression with NonSQLExpression {

  // TODO [NULL HANDLING]
  // if foldable, we might have NPEs during the analysis/optimization phase as rules like
  // ConstantFolding might call eval() directly
  override def foldable: Boolean = false

  override def nullable: Boolean = false

  override def dataType: DataType = child.dataType

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) throw new NullPointerException
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childExpr = child.genCode(ctx)
    val exprCode =
      code"""
      ${childExpr.code}
      if(${childExpr.isNull}) { throw new NullPointerException(); }
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${childExpr.value};
      """
    ev.copy(code = exprCode, isNull = FalseLiteral)
  }

}

/**
 * An expression that represents a reference to a struct.
 *
 * We need this since the initialization of an object on the bytecode level starts with pushing
 * an object ref to the operand stack. At that point, no fields are set.
 *
 * @param dataType the data type of this object ref.
 * @param nullable indicates if the struct is nullable.
 */
private[bytecode] case class StructRef(
    dataType: StructType,
    nullable: Boolean = false) extends LeafExpression with Unevaluable {

  private val fieldValueMap = new mutable.HashMap[String, Option[Expression]]()

  for (field <- dataType.fields) {
    fieldValueMap(field.name) = None
  }

  // TODO is it actually nullable? When I create it, it is guaranteed to be non-nullable
  // TODO what about nested data types then?
  // Fields might be nullable, through
  // def nullable: Boolean = fieldValueMap.keys.exists(_.nullable)
  // def nullable: Boolean = false

  def setField(fieldName: String, value: Expression): Unit = {
    fieldValueMap(fieldName) = Some(value)
  }

  def getField(fieldName: String): Option[Expression] = {
    fieldValueMap(fieldName)
  }

  def toStruct: CreateNamedStructUnsafe = {
    val structExprs = resolvedFieldValuedMap.flatMap { case (fieldName, value) =>
      Seq(Literal(fieldName), value.get)
    }
    CreateNamedStructUnsafe(structExprs.toSeq)
  }

  def assignFieldsToAttrs(attrs: Seq[Attribute]): Seq[Alias] = {
    attrs.map { attr =>
      resolvedFieldValuedMap(attr.name) match {
        case Some(nestedStructRef: StructRef) =>
          // TODO do you need a cast here?
          Alias(nestedStructRef.toStruct, attr.name)(exprId = attr.exprId)
        case Some(valueExpr) =>
          Alias(Cast(valueExpr, attr.dataType), attr.name)(exprId = attr.exprId)
          // TODO what if not set?
        case None =>
          throw new RuntimeException("Not expected")
      }
    }
  }

  private def resolvedFieldValuedMap =
    fieldValueMap.mapValues { value => value.map { _.transformUp(resolvePrimitiveWrappers) }}
}

/**
 * An expression that represents a reference to a Java primitive wrapper
 * such as [[java.lang.Integer]] or [[java.lang.Long]].
 *
 * This expression is needed to distinguish primitives from their wrappers during bytecode analysis.
 *
 * @param value the value of this expression.
 * @param dataType the data type of this expression.
 * @param nullable indicates if the expression is nullable.
 */
private[bytecode] case class PrimitiveWrapperRef(
    var value: Option[Expression] = None,
    dataType: DataType,
    nullable: Boolean = false) extends LeafExpression with Unevaluable

/**
 * An expression that represents a reference to a Scala object such as [[scala.Predef]].
 *
 * @param dataType the data type of the object.
 * @param nullable indicates if the expression is nullable.
 */
private[bytecode] case class ScalaObjectRef(
    dataType: ObjectType,
    nullable: Boolean = false) extends LeafExpression with Unevaluable
