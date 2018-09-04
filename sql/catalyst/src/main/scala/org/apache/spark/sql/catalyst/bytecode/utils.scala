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

import javassist.{ClassClassPath, ClassPool, CtClass}
import javassist.bytecode.Descriptor

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.types.DataType

/**
 * A base trait that denotes an outcome of evaluating a behavior (e.g., method).
 */
sealed trait Result

/**
 * A class that denotes a successful evaluation of a behavior.
 * The return value is optional (e.g., void functions, constructors).
 *
 * @param returnValue the optional return value.
 */
final case class Success(returnValue: Option[Expression]) extends Result

/**
 * A class that denotes an unsuccessful evaluation of a behavior.
 *
 * @param error the error message.
 */
final case class Failure(error: String) extends Result

/**
 * A class that represents a bytecode instruction defined at `opcodeIndex` in the provided behavior.
 *
 * @param opcodeIndex the index of the current instruction.
 * @param behavior the current behavior (e.g., a constructor or a method).
 */
case class Instruction(opcodeIndex: Int, behavior: Behavior) {

  lazy val opcode: Int = behavior.getOpcode(opcodeIndex)
  lazy val opcodeString: String = behavior.getOpcodeString(opcodeIndex)

  def next(): Instruction = {
    val nextOpcodeIndex = behavior.getNextOpcodeIndex(opcodeIndex)
    copy(opcodeIndex = nextOpcodeIndex)
  }
}

/**
 * An object that wraps [[ClassPool]] and allows to retrieve [[CtClass]]es.
 */
object CtClassPool {

  private lazy val classPool = ClassPool.getDefault

  def addClassPathFor(clazz: Class[_]): Unit = {
    val closureClassPath = new ClassClassPath(clazz)
    classPool.insertClassPath(closureClassPath)
  }

  def getCtClass(name: String): CtClass = {
    classPool.get(name)
  }

  // TODO existential types?
  def getCtClass(clazz: Class[_]): CtClass = {
    classPool.get(clazz.getName)
  }
}

/**
 * A class that holds closure metadata.
 *
 * @param inClass the input Java class.
 * @param inAttrs the input Catalyst attributes.
 * @param outClass the output Java class.
 * @param outAttrs the output Catalyst attributes.
 */
case class ClosureMetadata(
    inClass: Class[_],
    inAttrs: Seq[Attribute],
    outClass: Class[_],
    outAttrs: Seq[Attribute]) {

  lazy val inCtClass: CtClass = CtClassPool.getCtClass(inClass)
  lazy val outCtClass: CtClass = CtClassPool.getCtClass(outClass)
  lazy val methodDescriptor: String = Descriptor.ofMethod(outCtClass, Array(inCtClass))
  lazy val inType: DataType = getSparkType(inClass)
  lazy val outType: DataType = getSparkType(outClass)

  // TODO do we need to propagate the nullability info?
  private def getSparkType(clazz: Class[_]): DataType = {
    val tpe = ScalaReflection.getTypeFromClass(clazz)
    val schema = ScalaReflection.schemaFor(tpe)
    schema.dataType
  }
}
