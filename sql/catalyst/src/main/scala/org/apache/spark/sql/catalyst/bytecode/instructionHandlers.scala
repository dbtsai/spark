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

import javassist.bytecode.Opcode._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * The main object to handle bytecode instructions, which chains all instruction handlers.
 */
object InstructionHandler {

  private lazy val handlers = LocalVarInstructionHandler orElse
    ConstantLoadInstructionHandler orElse
    TypeConversionInstructionHandler orElse
    MathInstructionHandler orElse
    ComparisonInstructionHandler orElse
    StaticFieldInstructionHandler orElse
    ObjectFieldInstructionHandler orElse
    IfStatementInstructionHandler orElse
    MiscInstructionHandler orElse
    InvokeInstructionHandler orElse
    NewInstructionHandler orElse
    ReturnInstructionHandler

  def handle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack = new OperandStack): Result = {

    val resultOp = handlers.tryToHandle(instruction, localVars, operandStack)
    resultOp.getOrElse(Failure(s"An unsupported opcode: '${instruction.opcodeString}'"))
  }
}

/**
 * A base trait for all instruction handlers.
 *
 * Each handler must implement the `tryToHandle` method. If a handler can process an instruction, it
 * performs actions on the local variable array and the operand stack and returns [[Some]] with
 * the result of `InstructionHandler.handle` for the next instruction. If a handler
 * cannot process an instruction, `tryToHandle` should return None.
 *
 * Handlers can be chained via `orElse`.
 */
sealed trait InstructionHandler extends Logging {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result]

  def orElse(anotherHandler: InstructionHandler): InstructionHandler = {
    val currentHandler = this

    new InstructionHandler {
      def tryToHandle(i: Instruction, l: LocalVarArray, s: OperandStack): Option[Result] = {
        currentHandler.tryToHandle(i, l, s).orElse(anotherHandler.tryToHandle(i, l, s))
      }
    }
  }

  // TODO revise this, it is ugly
  /**
   * Creates a valid local variable array for a particular behavior by offloading the needed
   * number of expressions from the operand stack and arrangin them at correct indexes.
   *
   * Note that doubles and longs consume two slots in the local var array.
   * Objects and other primitives occupy only one.
   *
   * @param behavior the target behavior.
   * @param operandStack the operand stack.
   * @return the created local variable array.
   */
  def newLocalVarArray(behavior: Behavior, operandStack: OperandStack): LocalVarArray = {
    val numLocalVars = if (behavior.isStatic) behavior.numParameters else behavior.numParameters + 1

    // pop all needed local vars from the stack
    val operandExprs = new Array[Expression](numLocalVars)
    for (index <- numLocalVars - 1 to 0 by -1) {
      operandExprs(index) = operandStack.pop()
    }

    val localVars = new LocalVarArray
    var localVarIndex = 0

    val inputParams = if (behavior.isStatic) {
      operandExprs.zip(behavior.parameterTypes)
    } else {
      // set "this" for instance methods
      localVars(0) = operandExprs.head
      localVarIndex += 1
      operandExprs.drop(1).zip(behavior.parameterTypes)
    }

    for (inputParamIndex <- inputParams.indices) {
      val (operandExpr, operandCtClass) = inputParams(inputParamIndex)
      localVars(localVarIndex) = operandExpr
      localVarIndex += 1
      // primitive longs and doubles occupy two slots in the local variable array
      if (operandCtClass.getName == "long" || operandCtClass.getName == "double") {
        localVars(localVarIndex) = null
        localVarIndex += 1
      }
    }

    localVars
  }
}

/**
 * A handler that deals with all instructions related to loading local vars into the operand stack
 * and storing them into the local variable array.
 */
object LocalVarInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    // TODO do we need to differentiate between types? Any additional type check?
    val opcodeIndex = instruction.opcodeIndex
    val behavior = instruction.behavior

    // TODO
    // java.lang.Long will handled by ASTORE while primitives will occupy two slots and
    // will be handled by LSTORE
    // we are OK as it is but additional validation won't hurt

    instruction.opcode match {
      case ALOAD_0 | ILOAD_0 | LLOAD_0 | FLOAD_0 | DLOAD_0 => operandStack.push(localVars(0))
      case ALOAD_1 | ILOAD_1 | LLOAD_1 | FLOAD_1 | DLOAD_1 => operandStack.push(localVars(1))
      case ALOAD_2 | ILOAD_2 | LLOAD_2 | FLOAD_2 | DLOAD_2 => operandStack.push(localVars(2))
      case ALOAD_3 | ILOAD_3 | LLOAD_3 | FLOAD_3 | DLOAD_3 => operandStack.push(localVars(3))
      case ALOAD | ILOAD | LLOAD | FLOAD | DLOAD =>
        val localVarIndex = behavior.opcodes.byteAt(opcodeIndex + 1)
        val localVar = localVars(localVarIndex)
        operandStack.push(localVar)
      case ASTORE_0 | ISTORE_0 | LSTORE_0 | FSTORE_0 | DSTORE_0 => localVars(0) = operandStack.pop()
      case ASTORE_1 | ISTORE_1 | LSTORE_1 | FSTORE_1 | DSTORE_1 => localVars(1) = operandStack.pop()
      case ASTORE_2 | ISTORE_2 | LSTORE_2 | FSTORE_2 | DSTORE_2 => localVars(2) = operandStack.pop()
      case ASTORE_3 | ISTORE_3 | LSTORE_3 | FSTORE_3 | DSTORE_3 => localVars(3) = operandStack.pop()
      case ASTORE | ISTORE | LSTORE | FSTORE | DSTORE =>
        val localVarIndex = behavior.opcodes.byteAt(opcodeIndex + 1)
        localVars(localVarIndex) = operandStack.pop()
      case _ => return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }
}

/**
 * A handler that deals with pushing constants into the operand stack.
 */
object ConstantLoadInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    // TODO Byte, Char, Array?
    val behavior = instruction.behavior
    val opcodeIndex = instruction.opcodeIndex

    instruction.opcode match {
      case ICONST_0 => operandStack.push(Literal(0))
      case ICONST_1 => operandStack.push(Literal(1))
      case ICONST_2 => operandStack.push(Literal(2))
      case ICONST_3 => operandStack.push(Literal(3))
      case ICONST_4 => operandStack.push(Literal(4))
      case ICONST_5 => operandStack.push(Literal(5))
      case LCONST_0 => operandStack.push(Literal(0L))
      case LCONST_1 => operandStack.push(Literal(1L))
      case FCONST_0 => operandStack.push(Literal(0.0F))
      case FCONST_1 => operandStack.push(Literal(1.0F))
      case FCONST_2 => operandStack.push(Literal(2.0F))
      case DCONST_0 => operandStack.push(Literal(0.0))
      case DCONST_1 => operandStack.push(Literal(1.0))
      case LDC =>
        val constantIndex = behavior.opcodes.byteAt(opcodeIndex + 1)
        val constant = behavior.getConstant(constantIndex)
        operandStack.push(Literal(constant))
      case LDC_W | LDC2_W =>
        val constantIndex = behavior.opcodes.u16bitAt(opcodeIndex + 1)
        val constant = behavior.getConstant(constantIndex)
        operandStack.push(Literal(constant))
      case BIPUSH =>
        val byteValue = behavior.opcodes.byteAt(opcodeIndex + 1)
        operandStack.push(Literal(byteValue, IntegerType))
      // TODO: WARNING! It is not OK to keep it NullType
      case ACONST_NULL => operandStack.push(Literal(null, NullType))
      case _ => return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }

}

/**
 * A handler that deals with type conversions.
 */
object TypeConversionInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    instruction.opcode match {
      case L2I | F2I | D2I => operandStack.push(Cast(operandStack.pop(), IntegerType))
      case I2L | F2L | D2L => operandStack.push(Cast(operandStack.pop(), LongType))
      case I2F | L2F | D2F => operandStack.push(Cast(operandStack.pop(), FloatType))
      case I2D | L2D | F2D => operandStack.push(Cast(operandStack.pop(), DoubleType))
      case I2B => operandStack.push(Cast(operandStack.pop(), ByteType))
      case I2S => operandStack.push(Cast(operandStack.pop(), ShortType))
      // TODO string vs char type
      case I2C => operandStack.push(Cast(operandStack.pop(), StringType))
      case _ => return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }
}


/**
 * A handler that deals with basic math operations on items on the operand stack.
 */
object MathInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    instruction.opcode match {
      case IADD | LADD | FADD | DADD =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        operandStack.push(Add(leftOperand, rightOperand))
      case ISUB | LSUB | FSUB | DSUB =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        operandStack.push(Subtract(leftOperand, rightOperand))
      case IMUL | LMUL | FMUL | DMUL =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        operandStack.push(Multiply(leftOperand, rightOperand))
      case IDIV | LDIV | FDIV | DDIV =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        operandStack.push(Divide(leftOperand, rightOperand))
      case IREM | LREM | FREM | DREM =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        operandStack.push(Remainder(leftOperand, rightOperand))
      case _ => return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }
}

/**
 * A handler responsible for comparing items on the operand stack.
 *
 * FCMPG vs FCMPL and DCMPG vs DCMPL differ only in handling NaN values.
 */
object ComparisonInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    instruction.opcode match {
      case LCMP =>
        // TODO is it OK to have positive/negative or we need to have -1,0,1 only?
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        operandStack.push(Subtract(leftOperand, rightOperand))
      case FCMPG | DCMPG =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        val branches = Seq(
          Or(IsNaN(leftOperand), IsNaN(rightOperand)) -> Literal(1),
          GreaterThan(leftOperand, rightOperand) -> Literal(1),
          LessThan(leftOperand, rightOperand) -> Literal(-1))
        operandStack.push(CaseWhen(branches, Literal(0)))
      case FCMPL | DCMPL =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        val branches = Seq(
          Or(IsNaN(leftOperand), IsNaN(rightOperand)) -> Literal(-1),
          GreaterThan(leftOperand, rightOperand) -> Literal(1),
          LessThan(leftOperand, rightOperand) -> Literal(-1))
        operandStack.push(CaseWhen(branches, Literal(0)))
      case _ => return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }
}

/**
 * A handler for INVOKE opcodes.
 *
 * Also, this handler intercepts a few calls on primitive wrappers and replaces them with
 * equivalent Catalyst expressions. Those methods are very verbose and following their logic
 * instruction by instruction will complicate the final Catalyst expression.
 */
object InvokeInstructionHandler extends InstructionHandler {

  // TODO this is very limited for now
  // TODO extend once agree on a proper way to handle this
  private val staticMethodHandler: PartialFunction[Behavior, LocalVarArray => Expression] = {
    case b if b.declaringClass.getName == JAVA_LONG_CLASS && b.name == "valueOf" =>
      localVars => Cast(localVars(0), LongType)
    case b if b.declaringClass.getName == JAVA_LONG_CLASS && b.name == "toString" =>
      localVars => Cast(localVars(0), StringType)
  }

  private val instanceMethodHandler: PartialFunction[Behavior, LocalVarArray => Expression] = {
    case b if b.declaringClass.getName == JAVA_STRING_CLASS && b.name == "concat" =>
      localVars => Concat(Seq(NPEonNull(localVars(0)), NPEonNull(localVars(1))))
    case b if b.declaringClass.getName == JAVA_OBJECT_CLASS && b.name == "toString" =>
      localVars => Cast(NPEonNull(localVars(0)), StringType)
  }

  private def isSpecialBehavior(behavior: Behavior, localVars: LocalVarArray): Boolean = {
    if (behavior.isStatic) {
      staticMethodHandler.isDefinedAt(behavior)
    } else {
      val dataType = localVars(0).dataType
      isSupportedSimpleType(dataType) && instanceMethodHandler.isDefinedAt(behavior)
    }
  }

  private val specialBehaviorHandler = staticMethodHandler orElse instanceMethodHandler

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    val opcodeIndex = instruction.opcodeIndex
    val behavior = instruction.behavior

    instruction.opcode match {
      case INVOKEVIRTUAL | INVOKESPECIAL | INVOKESTATIC =>
        val targetBehavior = behavior.getBehaviorAt(opcodeIndex + 1)
        val targetLocalVars = newLocalVarArray(targetBehavior, operandStack)

        // ensure we do not call an instance method on a null reference
        if (!targetBehavior.isStatic && isNullLiteral(targetLocalVars(0))) {
          throw new AnalysisException(s"Calling '${targetBehavior.name}' on a null reference")
        }

        // intercept special cases such as calls on primitive wrappers
        if (isSpecialBehavior(targetBehavior, targetLocalVars)) {
          val resultExpr = specialBehaviorHandler(targetBehavior)(targetLocalVars)
          operandStack.push(resultExpr)
          val nextInstruction = instruction.next()
          return Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
        }

        val targetInstruction = Instruction(0, targetBehavior)
        val targetBehaviorResult = InstructionHandler.handle(targetInstruction, targetLocalVars)

        targetBehaviorResult match {
          case Success(returnValue) =>
            returnValue.foreach { value =>
              // TODO how will it behave with nested data types?
              if (targetBehavior.isStatic || !targetLocalVars(0).nullable) {
                operandStack.push(value)
              } else {
                val thisRef = targetLocalVars(0)
                val expr = If(IsNull(thisRef), NPEonNull(Literal(null, value.dataType)), value)
                operandStack.push(expr)
              }
            }
            val nextInstruction = instruction.next()
            Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
          case Failure(err) =>
            Some(Failure(s"$err while evaluating the target of ${instruction.opcodeString}: "))
        }
      case _ => None
    }
  }

  private def isNullLiteral(e: Expression): Boolean = e match {
    case Literal(null, _) => true
    case _ => false
  }
}

/**
 * A handler that works with static variables.
 */
object StaticFieldInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    val opcodeIndex = instruction.opcodeIndex
    val behavior = instruction.behavior

    instruction.opcode match {
      case GETSTATIC =>
        val field = behavior.getFieldAt(opcodeIndex + 1)
        // TODO Java statics
        // right now, only Scala objects are supported
        if (field.getName == "MODULE$") {
          val className = field.getDeclaringClass.getName
          val objectType = ObjectType(Utils.classForName(className))
          val scalaObjectRef = ScalaObjectRef(objectType)
          operandStack.push(scalaObjectRef)
          val nextInstruction = instruction.next()
          Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
        } else {
          Some(Failure(s"Getting arbitrary static fields is not supported: $field"))
        }
      case PUTSTATIC => Some(Failure(s"Setting static fields is not supported"))
      case _ => None
    }
  }
}

/**
 * A handler responsible for dealing with object fields.
 */
object ObjectFieldInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    val opcodeIndex = instruction.opcodeIndex
    val behavior = instruction.behavior

    instruction.opcode match {
      case GETFIELD =>
        val targetField = behavior.getFieldAt(opcodeIndex + 1)
        val operand = operandStack.pop()
        val targetFieldName = targetField.getName
        operand match {
          case structRef: StructRef =>
            // TODO [NULL HANDLING]
            val fieldExpr = structRef.getField(targetFieldName).orNull
            operandStack.push(fieldExpr)
          case e: Expression if isSupportedSimpleType(e.dataType) && targetFieldName == "value" =>
            operandStack.push(operand)
          case e: Expression if e.dataType.isInstanceOf[StructType] =>
            val structType = e.dataType.asInstanceOf[StructType]
            val fieldIndex = structType.fieldIndex(targetFieldName)
            operandStack.push(GetStructField(e, fieldIndex))
          case _ =>
            return Some(Failure(s"Cannot get field '$targetFieldName' from '$operand'"))
        }
      case PUTFIELD =>
        val fieldIndex = behavior.opcodes.u16bitAt(opcodeIndex + 1)
        val targetFieldName = behavior.constPool.getFieldrefName(fieldIndex)
        val (fieldValue, expression) = (operandStack.pop(), operandStack.pop())
        expression match {
          case structRef: StructRef =>
            structRef.setField(targetFieldName, fieldValue)
          case wrapperRef: PrimitiveWrapperRef =>
            wrapperRef.value = Some(fieldValue)
          case _ =>
            return Some(Failure(s"Cannot set $targetFieldName in $expression"))
        }
      case _ => return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }
}

/**
 * A handler for if statements.
 */
object IfStatementInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    val opcodeIndex = instruction.opcodeIndex
    val behavior = instruction.behavior

    def computeIfExpression(condition: Expression): Option[Result] = {
      val trueExprIndex = behavior.opcodes.s16bitAt(opcodeIndex + 1) + opcodeIndex
      val trueInstruction = instruction.copy(opcodeIndex = trueExprIndex)
      val trueExpr = InstructionHandler.handle(trueInstruction, localVars, operandStack)

      val falseExprIndex = behavior.getNextOpcodeIndex(opcodeIndex)
      val falseInstruction = instruction.copy(opcodeIndex = falseExprIndex)
      val falseExpr = InstructionHandler.handle(falseInstruction, localVars, operandStack)

      // TODO: what if we are working with structs?

      (trueExpr, falseExpr) match {
        case (Success(trueBranchExpr), Success(falseBranchExpr)) =>
          val returnValue = Some(If(condition, trueBranchExpr.get, falseBranchExpr.get))
          Some(Success(returnValue))
        case (Failure(errorMsg), _) =>
          Some(Failure(s"Could not evaluate the true branch of an if statement: $errorMsg"))
        case (_, Failure(errorMsg)) =>
          Some(Failure(s"Could not evaluate the false branch of an if statement: $errorMsg"))
      }
    }

    behavior.getOpcode(opcodeIndex) match {
      case IFEQ =>
        val top = operandStack.pop()
        computeIfExpression(EqualTo(top, Cast(Literal(0), top.dataType)))
      case IFNE =>
        val top = operandStack.pop()
        computeIfExpression(Not(EqualTo(top, Cast(Literal(0), top.dataType))))
      case IFGT =>
        val top = operandStack.pop()
        computeIfExpression(GreaterThan(top, Cast(Literal(0), top.dataType)))
      case IFGE =>
        val top = operandStack.pop()
        computeIfExpression(GreaterThanOrEqual(top, Cast(Literal(0), top.dataType)))
      case IFLT =>
        val top = operandStack.pop()
        computeIfExpression(LessThan(top, Cast(Literal(0), top.dataType)))
      case IFLE =>
        val top = operandStack.pop()
        computeIfExpression(LessThanOrEqual(top, Cast(Literal(0), top.dataType)))
      case IFNONNULL =>
        val top = operandStack.pop()
        computeIfExpression(IsNotNull(top))
      case IFNULL =>
        val top = operandStack.pop()
        computeIfExpression(IsNull(top))
      case IF_ACMPEQ | IF_ICMPEQ =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(EqualTo(leftOperand, rightOperand))
      case IF_ACMPNE | IF_ICMPNE =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(Not(EqualTo(leftOperand, rightOperand)))
      case IF_ICMPGE =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(GreaterThanOrEqual(leftOperand, rightOperand))
      case IF_ICMPGT =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(GreaterThan(leftOperand, rightOperand))
      case IF_ICMPLE =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(LessThanOrEqual(leftOperand, rightOperand))
      case IF_ICMPLT =>
        val (rightOperand, leftOperand) = (operandStack.pop(), operandStack.pop())
        computeIfExpression(LessThan(leftOperand, rightOperand))
      case _ =>
        None
    }
  }
}

/**
 * A handler for creating objects.
 */
object NewInstructionHandler extends InstructionHandler {

  // TODO: more types
  private val primitiveWrapperHandler: PartialFunction[String, Expression] = {
    case JAVA_INTEGER_CLASS => PrimitiveWrapperRef(dataType = IntegerType)
    case JAVA_LONG_CLASS => PrimitiveWrapperRef(dataType = LongType)
  }

  override protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    val opcodeIndex = instruction.opcodeIndex
    val behavior = instruction.behavior

    instruction.opcode match {
      case NEW =>
        val classIndex = behavior.opcodes.u16bitAt(opcodeIndex + 1)
        val className = behavior.constPool.getClassInfo(classIndex)

        if (primitiveWrapperHandler.isDefinedAt(className)) {
          val resultExpr = primitiveWrapperHandler(className)
          operandStack.push(resultExpr)
          val nextInstruction = instruction.next()
          return Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
        }

        val tpe = ScalaReflection.getTypeFromClass(Utils.classForName(className))
        // TODO: Scala tuples?
        val schema = ScalaReflection.schemaFor(tpe)
        schema.dataType match {
          case st: StructType =>
            operandStack.push(StructRef(st))
          case _ => throw new RuntimeException("Not supported now")
        }
      case _ =>
        return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }
}

/**
 * A handler that deals with miscellaneous instructions.
 */
object MiscInstructionHandler extends InstructionHandler {

  override protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    val opcodeIndex = instruction.opcodeIndex
    val behavior = instruction.behavior

    instruction.opcode match {
      case DUP =>
        operandStack.push(operandStack.top)
      case DUP2 =>
        operandStack.top match {
          case w: PrimitiveWrapperRef if w.dataType == LongType || w.dataType == DoubleType =>
            operandStack.push(operandStack.top)
          case _ =>
            val (topOperand, nextOperand) = (operandStack.pop(), operandStack.pop())
            operandStack.push(nextOperand)
            operandStack.push(topOperand)
            operandStack.push(nextOperand)
            operandStack.push(topOperand)
        }
      case POP =>
        operandStack.pop()
      case POP2 =>
        operandStack.top match {
          case w: PrimitiveWrapperRef if w.dataType == LongType || w.dataType == DoubleType =>
            operandStack.pop()
          case _ =>
            operandStack.pop()
            operandStack.pop()
        }
        operandStack.pop()
      case GOTO =>
        val targetIndex = behavior.opcodes.s16bitAt(opcodeIndex + 1) + opcodeIndex
        val nextInstruction = instruction.copy(opcodeIndex = targetIndex)
        return Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
      case CHECKCAST =>
        val classIndex = behavior.opcodes.u16bitAt(opcodeIndex + 1)
        val className = behavior.constPool.getClassInfo(classIndex)
        val tpe = ScalaReflection.getTypeFromClass(Utils.classForName(className))
        val schema = ScalaReflection.schemaFor(tpe)
        require(operandStack.top.dataType == schema.dataType)
      case _ => return None
    }

    val nextInstruction = instruction.next()
    Some(InstructionHandler.handle(nextInstruction, localVars, operandStack))
  }
}

/**
 * A handler that deals with return statements.
 */
object ReturnInstructionHandler extends InstructionHandler {

  protected def tryToHandle(
      instruction: Instruction,
      localVars: LocalVarArray,
      operandStack: OperandStack): Option[Result] = {

    val behavior = instruction.behavior

    // TODO: other smaller types that are still represented as ints
    instruction.opcode match {
      case IRETURN if behavior.returnType.exists(_.getName == "boolean") =>
        // in bytecode, boolean operations are represented as ints, so we need an explicit cast
        val expr = operandStack.top.transformUp(resolvePrimitiveWrappers)
        Some(Success(returnValue = Some(Cast(expr, BooleanType))))
      case ARETURN | IRETURN | LRETURN | FRETURN | DRETURN =>
        val expr = operandStack.top.transformUp(resolvePrimitiveWrappers)
        Some(Success(returnValue = Some(expr)))
      case RETURN =>
        Some(Success(returnValue = None))
      case _ => None
    }
  }
}
