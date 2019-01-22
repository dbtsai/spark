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

package org.apache.spark.sql

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.execution.{DeserializeToObjectExec, FilterExec, ProjectExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class PrimitiveTypesBytecodeAnalysisTest extends BytecodeAnalysisTest {
  import testImplicits._

  test("map transformations with basic arithmetic operations") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val byteDS = Seq(1.toByte, 2.toByte).toDS()
        .map(b => (b + 1).toByte)
        .map(b => (b - 6).toByte)
        .map(b => (b * 1).toByte)
        .map(b => (b / 2).toByte)
        .map(b => (b % 2).toByte)
      byteDS.explain(true)
      val bytePlan = byteDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(bytePlan.schema == byteDS.schema)
      assertNoSerialization(bytePlan)
      assert(bytePlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(byteDS, 0.toByte, -1.toByte)

      val shortDS = Seq(1.toShort, 2.toShort).toDS()
        .map(s => (s + 1).toShort)
        .map(s => (s - 6).toShort)
        .map(s => (s * 1).toShort)
        .map(s => (s / 2).toShort)
        .map(s => (s % 2).toShort)
      shortDS.explain(true)
      val shortPlan = shortDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(shortPlan.schema == shortDS.schema)
      assertNoSerialization(shortPlan)
      assert(shortPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(shortDS, 0.toShort, -1.toShort)

      val intDS = Seq(1, 2).toDS()
        .map(i => i + 1)
        .map(i => i - 6)
        .map(i => i * 1)
        .map(i => i / 2)
        .map(i => i % 2)
      intDS.explain(true)
      val intPlan = intDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(intPlan.schema == intDS.schema)
      assertNoSerialization(intPlan)
      assert(intPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(intDS, 0, -1)

      val longDS = spark.range(1, 3).as[Long]
        .map(l => l + 1)
        .map(l => l - 6)
        .map(l => l * 1)
        .map(l => l / 2)
        .map(l => l % 2)
      longDS.explain(true)
      val longPlan = longDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(longPlan.schema == longDS.schema)
      assertNoSerialization(longPlan)
      assert(longPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(longDS, 0L, -1L)

      val floatDS = Seq(1.0F, 2.0F).toDS()
        .map(f => f + 1)
        .map(f => f - 6)
        .map(f => f * 1)
        .map(f => f / 2)
        .map(f => f % 2)
      floatDS.explain(true)
      val floatPlan = floatDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(floatPlan.schema == floatDS.schema)
      assertNoSerialization(floatPlan)
      assert(floatPlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(floatDS, -0.0F, -1.5F)

      val doubleDS = Seq(1.0, 2.0).toDS()
        .map(d => d + 1)
        .map(d => d - 6)
        .map(d => d * 1)
        .map(d => d / 2)
        .map(d => d % 2)
      doubleDS.explain(true)
      val doublePlan = doubleDS.queryExecution.executedPlan
      // TODO: the field `value` is nullable as Divide is always nullable
      // we break the schema here
      // assert(doublePlan.schema == doubleDS.schema)
      assertNoSerialization(doublePlan)
      assert(doublePlan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(doubleDS, -0.0, -1.5)
    }

    // TODO: decimal, string, calendar, etc
  }

  // TODO: this one is broken as edge cases are not handled
  test("division by 0") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {

      // TODO: 1 / 0 -> An arithmetic exception
      val byteDS = Seq(1.toByte, 2.toByte).toDS()
      byteDS.map(b => b / 0).show()

      // TODO: 1 / 0.0 -> Infinity
      val doubleDS = Seq(1.0, 2.0).toDS()
      doubleDS.map(d => d / 0.0).show()
    }
  }
}

class JavaPrimitiveWrappersBytecodeAnalysisSuite extends BytecodeAnalysisTest {
  import testImplicits._

  test("map transformations with java.lang.Long") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 3).as[java.lang.Long]

      val q1 = ds
        .map { l => JavaLongWrapper(l) }
        .map { wrapper =>
          val oldNumber: java.lang.Long = wrapper.number
          val additionalValue = new java.lang.Long(3)
          wrapper.add(oldNumber, additionalValue)
          wrapper
        }
      q1.explain(true)
      val plan1 = q1.queryExecution.executedPlan
      assertNoSerialization(plan1)
      checkDataset(q1, JavaLongWrapper(5L), JavaLongWrapper(7L))

      val q2 = ds
        .map { l => JavaLongWrapper(l) }
        .map { wrapper =>
          val oldNumber: java.lang.Long = wrapper.number
          val primitiveValue: Long = 3L
          val additionalValue = wrapper.number + primitiveValue
          wrapper.add(oldNumber, additionalValue)
          wrapper
        }
      q2.explain(true)
      val plan2 = q2.queryExecution.executedPlan
      assertNoSerialization(plan2)
      checkDataset(q2, JavaLongWrapper(6L), JavaLongWrapper(9L))

      // TODO: call methods on java.lang.Long
    }
  }

  test("null handling") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 3).as[java.lang.Long]

      // TODO: this throws `Calling 'toString' on a null reference`
      val q1 = ds
        .map { _ =>
          val longValue: java.lang.Long = null
          longValue.toString
        }
      q1.explain(true)
      q1.show()

      // TODO: this is weird even without bytecode analysis. I would expect this to throw a NPE.
      val q2 = ds
        .map { l => if (l == 1) l else null }
        .map { l => l.toString }
      q2.explain(true)
      q2.show()

      // TODO: this must throw a NPE but it doesn't because we do not have proper null handling yet
      val q3 = ds
        .map { l => JavaLongWrapper(l) }
        .map { wrapper =>
          val oldNumber: java.lang.Long = wrapper.number
          if (oldNumber == 1) wrapper else JavaLongWrapper(null)
        }
        .map { wrapper =>
          wrapper.number.toString
        }
      q3.explain(true)
      q3.show()
    }
  }
}

class CaseClassesBytecodeAnalysisSuite extends BytecodeAnalysisTest {
  import testImplicits._

  test("basic map transformations") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val q1 = spark.range(1, 3).as[Long]
        .map(l => Event(l, l.toString))
        .map(event => Item(event.l))
      q1.explain(true)
      q1.show()
      val plan1 = q1.queryExecution.executedPlan
      assert(plan1.schema == q1.schema)
      assertNoSerialization(plan1)
      assert(plan1.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(q1, Item(1), Item(2))

      val q2 = Seq(Event(1L, "1L")).toDS()
        .map(event => Item(event.l))
        .map(item => Event(item.i, item.i.toString))
      q2.explain(true)
      q2.show()
      val plan2 = q2.queryExecution.executedPlan
      // TODO: we derive a more precise schema in this case
      // assert(plan2.schema == q2.schema)
      assertNoSerialization(plan2)
      assert(plan2.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(q2, Event(1L, "1"))
    }
  }

  test("map transformations with nested data") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 3).as[Long]

      // TODO: fails
      // `Cannot get field 'i' from 'if (isnull(item#10)) null else structref(class Item, false)'`
      // This is because our deserializer is complicated
      // TODO: any chance we can do it after we removed the deserialization step?
      // == Optimized Logical Plan ==
      // SerializeFromObject [input[0, bigint, false] AS value#17L]
      // +- MapElements org.apache.spark.sql.CaseClassesBytecodeAnalysisSuite$$Lambda$1039/8415638@65ddee5a, class org.apache.spark.sql.ItemWrapper, [StructField(name,StringType,true), StructField(item,StructType(StructField(i,LongType,false)),true)], obj#16: bigint
      // +- MapElements org.apache.spark.sql.CaseClassesBytecodeAnalysisSuite$$Lambda$998/401862395@43fda8d9, long, [StructField(value,LongType,false)], obj#8: org.apache.spark.sql.ItemWrapper
      // +- DeserializeToObject id#0: bigint, obj#7: bigint
      // +- Range (1, 3, step=1, splits=Some(2))
      val q1 = ds
        .map { l => ItemWrapper(l.toString, Item(l)) }
        .map { iw => iw.item.i }
      q1.explain(true)
      q1.show()
      val plan1 = q1.queryExecution.executedPlan
      assert(plan1.schema == q1.schema)
      assertNoSerialization(plan1)
      assert(plan1.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(q1, 1L, 2L)

      // TODO: same issue as above
      val q2 = ds
        .map { l => ItemWrapper(l.toString, Item(l)) }
        .map { iw => Event(iw.item.i, iw.name) }
      q2.explain(true)
      q2.show()
      val plan2 = q2.queryExecution.executedPlan
      assert(plan2.schema == q2.schema)
      assertNoSerialization(plan2)
      assert(plan2.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(q2, Event(1, "1"), Event(2, "2"))

      // TODO: same issue as above
      val q3 = ds
        .map { l => ItemWrapper(l.toString, Item(l)) }
        .map { iw => ItemWrapperIndex(10, iw) }
        .map { iwi => iwi.itemWrapper.item.i }
      q3.explain(true)
      q3.show()
      val plan3 = q3.queryExecution.executedPlan
      assert(plan3.schema == q3.schema)
      assertNoSerialization(plan3)
      assert(plan3.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(q3, 1L, 2L)
    }
  }

  test("no additional serialization is introduced") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 10).as[Long]

      val q1 = ds
        .map { l => println("unsupported"); l + 1 } // scalastyle:ignore
        .map { l => l + 6 }
        .map { l => println("unsupported"); l + 5 } // scalastyle:ignore
      val plan1 = q1.queryExecution.executedPlan
      assert(plan1.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan1.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan1.collect { case p: ProjectExec => p }.isEmpty)

      val q2 = ds
        .map { l => println("unsupported"); l + 1 } // scalastyle:ignore
        .filter(_ > 4)
        .map { l => println("unsupported"); l + 5 } // scalastyle:ignore
      val plan2 = q2.queryExecution.executedPlan
      assert(plan2.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan2.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan2.collect { case f: FilterExec if f.condition.isInstanceOf[Invoke] => f }.nonEmpty)

      val q3 = ds
        .map { l => println("unsupported"); l + 1 } // scalastyle:ignore
        .filter(_ > 4)
      val plan3 = q3.queryExecution.executedPlan
      assert(plan3.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan3.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan3.collect { case f: FilterExec if f.condition.isInstanceOf[Invoke] => f }.isEmpty)

      val q4 = ds
        .map { l => println("unsupported"); Event(l, l.toString) } // scalastyle:ignore
        .map { event => Event(event.l + 1, event.s) }
        .map { event => println("unsupported"); event.l } // scalastyle:ignore
      val plan4 = q4.queryExecution.executedPlan
      assert(plan4.collect { case d: DeserializeToObjectExec => d }.size == 1)
      assert(plan4.collect { case s: SerializeFromObjectExec => s }.size == 1)
      assert(plan4.collect { case p: ProjectExec => p }.isEmpty)
    }
  }

  test("null handling") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 10).as[Long]

      val q1 = ds
        .map(l => Event(l, null))
        .map(event => event.s.concat("suffix"))
      val plan1 = q1.queryExecution.executedPlan
      // TODO: we derive a more precise schema
      // assert(plan1.schema == q1.schema)
      assertNoSerialization(plan1)
      val thrownException1 = intercept[SparkException] {
        q1.collect()
      }
      assert(thrownException1.getCause.isInstanceOf[NullPointerException])

      val q2 = ds
        .map { l =>
          val e: Event = null
          e.doSmth()
          Event(l, l.toString)
        }
      // TODO: this throws an analysis exception without execution
      // val plan2 = q2.queryExecution.executedPlan
      // assert(plan2.schema == q2.schema)
      val thrownException2 = intercept[AnalysisException] {
        q2.collect()
      }
      assert(thrownException2.message == "Calling 'doSmth' on a null reference")

      val q3 = ds
        .map { l =>
          val e: Event = EventHelper.nullEvent()
          e.copy(l = l + 1)
        }
      // TODO: this throws an analysis exception without execution
      // val plan3 = q3.queryExecution.executedPlan
      // assert(plan3.schema == q3.schema)
      val thrownException3 = intercept[AnalysisException] {
        q3.collect()
      }
      assert(thrownException3.message == "Calling 'copy$default$2' on a null reference")

      val q4 = ds
        .map { l =>
          val e = Event(l, l.toString)
          val newE = e.generate()
          newE.copy(l = l + 1)
        }
      // TODO: this throws an analysis exception without execution
      // val plan4 = q4.queryExecution.executedPlan
      // assert(plan4.schema == q4.schema)
      val thrownException4 = intercept[AnalysisException] {
        q4.collect()
      }
      assert(thrownException4.message == "Calling 'copy$default$2' on a null reference")

      // TODO this one is currently broken because of complicated deserializers
      // 'if (isnull(item#52)) null else structref(class org.apache.spark.sql.Item, false)'
      val q5 = ds
        .map { l =>
          if (l % 2 == 0) ItemWrapper(l.toString, Item(l)) else ItemWrapper(l.toString, null)
        }
        .map { iw => Event(iw.item.i, iw.name) }
      val plan5 = q5.queryExecution.executedPlan
      assert(plan5.schema == q5.schema)
      assertNoSerialization(q5.queryExecution.executedPlan)
      val thrownException5 = intercept[SparkException] {
        q5.collect()
      }
      assert(thrownException5.getCause.isInstanceOf[NullPointerException])
    }
  }
}

abstract class BytecodeAnalysisTest extends QueryTest with SharedSQLContext {

  protected def assertNoSerialization(plan: SparkPlan): Unit = {
    val isDeserializationAbsent = plan.collect { case d: DeserializeToObjectExec => d }.isEmpty
    assert(isDeserializationAbsent)
    val isSerializationAbsent = plan.collect { case s: SerializeFromObjectExec => s }.isEmpty
    assert(isSerializationAbsent)
  }
}

// Java primitive wrappers
case class JavaLongWrapper(var number: java.lang.Long) {
  def add(firstNumber: java.lang.Long, secondNumber: java.lang.Long): Unit = {
    number += (firstNumber + secondNumber)
  }
}

// nested data
case class Item(i: Long)
case class ItemWrapper(name: String, item: Item)
case class ItemWrapperIndex(index: Int, itemWrapper: ItemWrapper)

// null handling
case class Event(l: Long, s: String) {
  def doSmth(): Long = l * 2
  def generate(): Event = null
  def voidFunc(): Unit = {}
}
object EventHelper {
  def nullEvent(): Event = null
}
