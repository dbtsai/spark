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

package org.apache.spark.sql.catalyst

import scala.collection.mutable
import scala.util.Try

import javassist.{CtBehavior, CtClass, CtField, CtMethod, Modifier}
import javassist.bytecode.{CodeIterator, ConstPool}
import javassist.bytecode.InstructionPrinter.instructionString

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}

package object bytecode {

  // the size of the local var array depends on the number and size of local variables
  // and formal method parameters, so we cannot pre-compute it in advance.
  // hence, we are using an instance of mutable.HashMap[Int, Expression] instead of an array
  type LocalVarArray = mutable.HashMap[Int, Expression]

  type OperandStack = mutable.ArrayStack[Expression]

  val JAVA_BOOLEAN_CLASS = "java.lang.Boolean"
  val JAVA_BYTE_CLASS = "java.lang.Byte"
  val JAVA_SHORT_CLASS = "java.lang.Short"
  val JAVA_INTEGER_CLASS = "java.lang.Integer"
  val JAVA_LONG_CLASS = "java.lang.Long"
  val JAVA_FLOAT_CLASS = "java.lang.Float"
  val JAVA_DOUBLE_CLASS = "java.lang.Double"
  val JAVA_STRING_CLASS = "java.lang.String"
  val JAVA_OBJECT_CLASS = "java.lang.Object"

  private val supportedSimpleTypes = Seq(
    BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType)

  def isSupportedSimpleType(dataType: DataType): Boolean = supportedSimpleTypes.contains(dataType)

  def resolvePrimitiveWrappers: PartialFunction[Expression, Expression] = {
    // TODO: do we need to wrap this into NPEOnNull?
    case p: PrimitiveWrapperRef => Cast(p.value.get, p.dataType)
  }
  /**
   * This is a wrapper around [[CtBehavior]] to simplify the interaction.
   */
  implicit class Behavior(ctBehavior: CtBehavior) {

    lazy val name: String = ctBehavior.getName
    lazy val declaringClass: CtClass = ctBehavior.getDeclaringClass
    lazy val opcodes: CodeIterator = ctBehavior.getMethodInfo.getCodeAttribute.iterator()
    lazy val constPool: ConstPool = ctBehavior.getMethodInfo.getConstPool
    lazy val isStatic: Boolean = Modifier.isStatic(ctBehavior.getModifiers)
    lazy val numParameters: Int = ctBehavior.getParameterTypes.length
    lazy val parameterTypes: Array[CtClass] = ctBehavior.getParameterTypes
    lazy val returnType: Option[CtClass] = ctBehavior match {
      case method: CtMethod => Some(method.getReturnType)
      case _ => None
    }

    def getOpcodeString(index: Int): String = instructionString(opcodes, index, constPool)

    def getOpcode(index: Int): Int = opcodes.byteAt(index)

    // TODO return Option?
    def getNextOpcodeIndex(index: Int): Int = {
      opcodes.move(index)
      opcodes.next()
      opcodes.next()
    }

    def getConstant(index: Int): Any = {
      constPool.getTag(index) match {
        case ConstPool.CONST_Double => constPool.getDoubleInfo(index)
        case ConstPool.CONST_Float => constPool.getFloatInfo(index)
        case ConstPool.CONST_Integer => constPool.getIntegerInfo(index)
        case ConstPool.CONST_Long => constPool.getLongInfo(index)
        case ConstPool.CONST_String => constPool.getStringInfo(index)
      }
    }

    def getBehaviorAt(index: Int): Behavior = {
      val behaviorConstPoolIndex = opcodes.u16bitAt(index)
      val behaviorClassName = constPool.getMethodrefClassName(behaviorConstPoolIndex)
      val name = constPool.getMethodrefName(behaviorConstPoolIndex)
      val descriptor = constPool.getMethodrefType(behaviorConstPoolIndex)
      val ctClass = CtClassPool.getCtClass(behaviorClassName)
      val method = Try(ctClass.getMethod(name, descriptor))
      val constructor = Try(ctClass.getConstructor(descriptor))
      Behavior(method.orElse(constructor).get)
    }

    def getFieldAt(index: Int): CtField = {
      val fieldConstPoolIndex = opcodes.u16bitAt(index)
      val className = constPool.getFieldrefClassName(fieldConstPoolIndex)
      val fieldName = constPool.getFieldrefName(fieldConstPoolIndex)
      val ctClass = CtClassPool.getCtClass(className)
      ctClass.getField(fieldName)
    }

    // TODO consider a built-in solution from javassist
    def toDebugString: String = {
      val stringBuilder = new StringBuilder()

      val name = ctBehavior.getName
      val behaviorClass = ctBehavior.getDeclaringClass.getName
      val descriptor = ctBehavior.getMethodInfo.getDescriptor
      val isConstructor = ctBehavior.getMethodInfo.isConstructor
      stringBuilder.append(
        s"""Name: $name,
           |class: $behaviorClass,
           |isConstructor: $isConstructor,
           |descriptor: $descriptor\n""".stripMargin)

      val codes = ctBehavior.getMethodInfo.getCodeAttribute.iterator()
      codes.begin()
      while (codes.hasNext) {
        val pos = codes.next()
        val code = instructionString(codes, pos, constPool)
        stringBuilder.append(s"$pos: $code\n")
      }

      stringBuilder.toString()
    }
  }

}
