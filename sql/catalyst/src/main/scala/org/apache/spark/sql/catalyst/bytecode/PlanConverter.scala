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

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, Filter, MapElements, Project, SerializeFromObject, TypedFilter}
import org.apache.spark.sql.types.{BooleanType, ObjectType, StructType}
import org.apache.spark.util.Utils

/**
 * A utility class that converts logical plan nodes that involve typed operations into equivalent
 * logical plan nodes with native Catalyst expressions.
 */
object PlanConverter extends Logging {

  /**
   * Converts an instance of [[MapElements]] to an optional [[Project]] with native Catalyst
   * expressions by analyzing the map closure bytecode.
   *
   * If there is an unsupported opcode, the result will be None.
   *
   * @param serialize the node that converts output objects of the transformation into unsafe rows.
   * @param map the typed transformation to apply.
   * @param deserialize the node that converts unsafe rows into objects before the transformation.
   * @return an optional [[Project]] with native Catalyst expressions.
   */
  def convertMapElements(
      serialize: SerializeFromObject,
      map: MapElements,
      deserialize: DeserializeToObject): Option[Project] = {

    val inClass = map.argumentClass
    val inAttrs = deserialize.child.output
    val outClass = ScalaReflection.dataTypeJavaClass(map.outputObjAttr.dataType)
    val outAttrs = serialize.output
    val metadata = ClosureMetadata(inClass, inAttrs, outClass, outAttrs)

    convertClosure(map.func, metadata).flatMap {
      case s: StructRef =>
        val aliases = s.assignFieldsToAttrs(outAttrs)
        Some(Project(aliases, deserialize.child))
      case e if outAttrs.size == 1 && outAttrs.head.dataType == e.dataType =>
        val outAttr = outAttrs.head
        val alias = Alias(e, outAttr.name)(exprId = outAttr.exprId)
        Some(Project(Seq(alias), deserialize.child))
      case e if e.dataType.isInstanceOf[StructType] =>
        logDebug(s"$e is not a struct ref")
        None
      case e =>
        logDebug(s"$e cannot represent $outAttrs")
        None
    }
  }

  /**
   * Converts an instance of [[TypedFilter]] into an optional [[Filter]] with native Catalyst
   * expressions by analyzing the filter closure bytecode.
   *
   * If there is an unsupported opcode, the result will be None.
   *
   * @param typedFilter the filter to convert.
   * @return an optional [[Filter]] with native Catalyst expressions.
   */
  def convertTypedFilter(typedFilter: TypedFilter): Option[Filter] = {
    val inClass = typedFilter.argumentClass
    val inAttrs = typedFilter.child.output
    val outClass = ScalaReflection.dataTypeJavaClass(BooleanType)
    val outAttrs = typedFilter.output
    val metadata = ClosureMetadata(inClass, inAttrs, outClass, outAttrs)
    val filterExpr = convertClosure(typedFilter.func, metadata)
    filterExpr.foreach(expr => require(expr.dataType == BooleanType))
    filterExpr.map(Filter(_, typedFilter.child))
  }

  private def convertClosure(closure: AnyRef, metadata: ClosureMetadata): Option[Expression] = {
    try {
      val method = getMethod(closure, metadata.methodDescriptor)
      val localVars = getLocalVars(method, metadata)
      val instruction = Instruction(opcodeIndex = 0, method)
      InstructionHandler.handle(instruction, localVars) match {
        case Success(closureExpr) =>
          logDebug(s"Derived $closureExpr from ${method.toDebugString}")
          closureExpr
        case Failure(errorMsg) =>
          logWarning(errorMsg)
          None
      }
    } catch {
      case e: Exception if !e.isInstanceOf[AnalysisException] =>
        logWarning(s"Could not convert the closure: ${e.getMessage}")
        None
    }
  }

  private def getMethod(closure: AnyRef, methodDescriptor: String): Behavior = closure match {
    case _: MapFunction[_, _] =>
      throw new RuntimeException("Java closures are not supported now")
    case _ if Utils.scalaMajorVersion == "2.11" =>
      // Scala 2.11 compiler creates an anonymous class for every closure
      val closureClass = closure.getClass
      CtClassPool.addClassPathFor(closureClass)
      val closureCtClass = CtClassPool.getCtClass(closureClass)
      closureCtClass.getMethod("apply", methodDescriptor)
    case _ =>
      // Scala 2.12 uses Java 8 lambdas for closures
      Utils.getSerializedLambda(closure) match {
        case Some(lambda) =>
          val capturingClassName = lambda.getCapturingClass.replace('/', '.')
          val implMethodName = lambda.getImplMethodName.stripSuffix("$adapted")
          val capturingClass = Utils.classForName(capturingClassName)
          CtClassPool.addClassPathFor(capturingClass)
          val closureCtClass = CtClassPool.getCtClass(capturingClass)
          closureCtClass.getMethod(implMethodName, methodDescriptor)
        case _ =>
          throw new RuntimeException("Could not get a serialized lambda from the closure")
      }
  }

  private def getLocalVars(behavior: Behavior, metadata: ClosureMetadata): LocalVarArray = {
    val localVars = new LocalVarArray
    val inAttrs = metadata.inAttrs

    val inVariable = metadata.inType match {
      case structType: StructType =>
        val structRef = StructRef(structType)
        inAttrs.foreach { inAttr =>
          val attrName = inAttr.name
          structRef.setField(attrName, inAttr)
        }
        structRef
      case _ =>
        // if input is not a struct, we must have only one attribute that is used
        require(inAttrs.size == 1)
        inAttrs.head
    }

    if (behavior.isStatic) {
      localVars(0) = inVariable
    } else {
      val behaviorClass = Utils.classForName(behavior.declaringClass.getName)
      val thisRef = AttributeReference("this", ObjectType(behaviorClass))()
      localVars(0) = thisRef
      localVars(1) = inVariable
    }

    localVars
  }
}
