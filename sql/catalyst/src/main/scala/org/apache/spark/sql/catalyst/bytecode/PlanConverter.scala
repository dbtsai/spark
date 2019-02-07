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

import javassist.bytecode.Descriptor

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance, StaticInvoke}
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, Filter, MapElements, Project, SerializeFromObject, TypedFilter}
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}
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
   * If there is an unsupported bytecode instruction, the result will be [[None]].
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
    val outClass = ScalaReflection.dataTypeJavaClass(map.outputObjAttr.dataType)
    val closureMetadata = ClosureMetadata(Seq(inClass), outClass)
    val method = getMethod(map.func, closureMetadata)

    val thisRef = getThisRef(method)
    val inExpr = convertDeserializer(deserialize.deserializer)
    val localVars = newLocalVarArray(method, thisRef, Seq(inExpr))

    val outAttrs = serialize.output

    val mapExpr = convertToExpr(method, localVars)
    mapExpr.flatMap {
      case o: ObjectRef =>
        val aliases = o.assignFieldsToAttrs(outAttrs)
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
   * If there is an unsupported bytecode instruction, the result will be [[None]].
   *
   * @param typedFilter the filter to convert.
   * @return an optional [[Filter]] with native Catalyst expressions.
   */
  def convertTypedFilter(typedFilter: TypedFilter): Option[Filter] = {
    val inClass = typedFilter.argumentClass
    val outClass = ScalaReflection.dataTypeJavaClass(BooleanType)
    val closureMetadata = ClosureMetadata(Seq(inClass), outClass)
    val method = getMethod(typedFilter.func, closureMetadata)

    val thisRef = getThisRef(method)
    val inExpr = convertDeserializer(typedFilter.deserializer)
    val localVars = newLocalVarArray(method, thisRef, Seq(inExpr))

    val filterExpr = convertToExpr(method, localVars)
    filterExpr.foreach(expr => require(expr.dataType == BooleanType))
    filterExpr.map(Filter(_, typedFilter.child))
  }

  private def convertToExpr(method: Behavior, localVars: LocalVarArray): Option[Expression] = {
    handle(method, localVars) match {
      case Success(closureExpr) =>
        logDebug(s"Derived $closureExpr from ${method.toDebugString}")
        closureExpr.map(_.transformUp(resolvePrimitiveWrappers))
      case Failure(errorMsg) =>
        logWarning(errorMsg)
        None
    }
  }

  private def handle(method: Behavior, localVars: LocalVarArray): Result = {
    try {
      if (SpecialBehaviorHandler.canHandle(method, localVars)) {
        return Success(SpecialBehaviorHandler.handle(method, localVars))
      }
      val instruction = Instruction(opcodeIndex = 0, method)
      InstructionHandler.handle(instruction, localVars)
    } catch {
      case e: Exception if !e.isInstanceOf[AnalysisException] =>
        Failure(s"Could not convert the closure: ${e.getMessage}")
    }
  }

  private def getMethod(closure: AnyRef, metadata: ClosureMetadata): Behavior = closure match {
    case _: MapFunction[_, _] =>
      throw new RuntimeException("Java closures are not supported now")
    case _ if Utils.isScala2_11 =>
      // Scala 2.11 compiler creates an anonymous class for every closure
      val closureClass = closure.getClass
      CtClassPool.addClassPathFor(closureClass)
      val closureCtClass = CtClassPool.getCtClass(closureClass)
      closureCtClass.getMethod("apply", metadata.methodDescriptor)
    case _ =>
      // Scala 2.12 uses Java 8 lambdas for closures
      Utils.getSerializedLambda(closure) match {
        case Some(lambda) =>
          val capturingClassName = lambda.getCapturingClass.replace('/', '.')
          val implMethodName = lambda.getImplMethodName.stripSuffix("$adapted")
          val capturingClass = Utils.classForName(capturingClassName)
          CtClassPool.addClassPathFor(capturingClass)
          val closureCtClass = CtClassPool.getCtClass(capturingClass)
          closureCtClass.getMethod(implMethodName, metadata.methodDescriptor)
        case _ =>
          throw new RuntimeException("Could not get a serialized lambda from the closure")
      }
  }

  private def convertDeserializer(deserializer: Expression): Expression = deserializer transformUp {
    // TODO: other nodes
    // TODO: do we need the remaining fields in Invoke?
    case i @ Invoke(targetObject, methodName, outType, args, _, _) =>
       val inClasses = getInClasses(args)
       val outClass = ScalaReflection.dataTypeJavaClass(outType)
       val closureMetadata = ClosureMetadata(inClasses, outClass)
       val methodDescriptor = closureMetadata.methodDescriptor

       // TODO: is it really a valid way to do it?
       val clazz = ScalaReflection.dataTypeJavaClass(targetObject.dataType)
       val ctClass = CtClassPool.getCtClass(clazz)
       val method = ctClass.getMethod(methodName, methodDescriptor)

       val thisRef = Some(targetObject)
       val localVars = newLocalVarArray(method, thisRef, args)
       handle(method, localVars) match {
         case Success(expr) =>
           require(expr.nonEmpty, "Invoke cannot result in an empty expr")
           // TODO: do wee need to remove wrappers?
           expr.get
         case Failure(error) =>
           logDebug(error)
           i
       }
    // TODO: do we need the remaining fields in StaticInvoke?
    case i @ StaticInvoke(clazz, outType, methodName, args, _, _) =>
      CtClassPool.addClassPathFor(clazz)
      val inClasses = getInClasses(args)
      val outClass = ScalaReflection.dataTypeJavaClass(outType)
      val closureMetadata = ClosureMetadata(inClasses, outClass)
      val methodDescriptor = closureMetadata.methodDescriptor

      val ctClass = CtClassPool.getCtClass(clazz)
      val method = ctClass.getMethod(methodName, methodDescriptor)

      val localVars = newLocalVarArray(method, None, args)
      handle(method, localVars) match {
        case Success(expr) =>
          require(expr.nonEmpty, "StaticInvoke cannot result in an empty expr")
          // TODO: do wee need to remove wrappers?
          expr.get
        case Failure(error) =>
          logDebug(error)
          i
      }
    // TODO: do we need the remaining fields in NewInstance?
    case n @ NewInstance(clazz, args, _, _, _) =>
      CtClassPool.addClassPathFor(clazz)
      val inClasses = getInClasses(args)
      val argCtClasses = inClasses.map(CtClassPool.getCtClass)
      val constructorDescriptor = Descriptor.ofConstructor(argCtClasses.toArray)

      val ctClass = CtClassPool.getCtClass(clazz)
      val constructor = ctClass.getConstructor(constructorDescriptor)

      val thisRef = getThisRef(constructor)
      val localVars = newLocalVarArray(constructor, thisRef, args)
      handle(constructor, localVars) match {
        case Success(_) =>
          // return initialized `this` that was passed to the constructor
          // TODO: do wee need to remove wrappers?
          thisRef.get
        case Failure(error) =>
          logDebug(error)
          n
      }
    case other =>
      other
  }

  private def getThisRef(behavior: Behavior): Option[Expression] = behavior match {
    case b: Behavior if b.isStatic =>
      None
    case b: Behavior if b.isConstructor =>
      val clazz = Utils.classForName(behavior.declaringClass.getName)
      Some(ObjectRef(clazz))
    case b: Behavior =>
      throw new RuntimeException("instance methods are not supported now")
      // val behaviorClass = Utils.classForName(b.declaringClass.getName)
      // val dataType = ObjectType(behaviorClass)
      // Some(AttributeReference("this", dataType)())
  }

  private def getInClasses(inExprs: Seq[Expression]): Seq[Class[_]] = inExprs.map {
    case p: PrimitiveWrapperRef => ScalaReflection.javaBoxedType(p.dataType)
    case o if o.dataType == StringType => classOf[String]
    case other => ScalaReflection.dataTypeJavaClass(other.dataType)
  }
}
