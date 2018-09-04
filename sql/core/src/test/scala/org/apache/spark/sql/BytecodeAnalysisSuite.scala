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
import org.apache.spark.sql.types.StructType

case class Item(i: Long)
case class ItemWrapper(name: String, item: Item)
case class ItemWrapperIndex(index: Int, itemWrapper: ItemWrapper)
case class Event(l: Long, s: String) {
  def doSmth(): Long = l * 2
  def generate(): Event = null
}
object EventHelper {
  def nullEvent(): Event = null
}
case class JavaLongWrapper(var number: java.lang.Long) {
  def add(firstNumber: java.lang.Long, secondNumber: java.lang.Long): Unit = {
    number += firstNumber
    number += secondNumber
  }
}

// TODO add schema verifications
class BytecodeAnalysisSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("map transformations (primitives)") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      def ds: Dataset[Long] = spark.range(1, 3).as[Long]
          .map(l => l + 1)
          .map(l => l + 6)
      val plan = ds.queryExecution.executedPlan
      assertNoSerialization(plan)
      assert(plan.schema == getValidSchema(ds))
      assert(plan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(ds, 8L, 9L)

      // TODO more data types
    }
  }

  test("map transformations (case classes)") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      def ds: Dataset[Item] = spark.range(1, 3).as[Long]
        .map(l => Event(l, l.toString))
        .map(event => Item(event.l))
      val plan = ds.queryExecution.executedPlan
      assertNoSerialization(plan)
      assert(plan.schema == getValidSchema(ds))
      assert(plan.collect { case p: ProjectExec => p }.size == 1)
      checkDataset(ds, Item(1), Item(2))
    }
  }

  test("map transformations (nested data)") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 3).as[Long]

      def q1: Dataset[Long] = ds
        .map { l => ItemWrapper(l.toString, Item(l)) }
        .map { iw => iw.item.i}
      val plan1 = q1.queryExecution.executedPlan
      assertNoSerialization(plan1)
      // assert(plan1.schema == getValidSchema(q1))
      checkDataset(q1, 1L, 2L)

      def q2: Dataset[Event] = ds
        .map { l => ItemWrapper(l.toString, Item(l)) }
        .map { iw => Event(iw.item.i, iw.name) }
      val plan2 = q2.queryExecution.executedPlan
      assertNoSerialization(plan2)
      // assert(plan2.schema == getValidSchema(q2))
      checkDataset(q2, Event(1, "1"), Event(2, "2"))

      def q3: Dataset[Long] = ds
        .map { l => ItemWrapper(l.toString, Item(l)) }
        .map { iw => ItemWrapperIndex(10, iw)}
        .map { iwi => iwi.itemWrapper.item.i }
      val plan3 = q3.queryExecution.executedPlan
      assertNoSerialization(plan3)
      // assert(plan3.schema == getValidSchema(q3))
      checkDataset(q3, 1L, 2L)
    }
  }

  test("Java wrapper classes") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 3).as[java.lang.Long]

      def q1: Dataset[JavaLongWrapper] = ds
        .map { l => JavaLongWrapper(l) }
        .map { wrapper =>
          val oldNumber: java.lang.Long = wrapper.number
          val additionalValue = new java.lang.Long(3)
          wrapper.add(oldNumber, additionalValue)
          wrapper
        }

      val plan1 = q1.queryExecution.executedPlan
      assertNoSerialization(plan1)
      checkDataset(q1, JavaLongWrapper(5L), JavaLongWrapper(7L))

      def q2: Dataset[JavaLongWrapper] = ds
        .map { l => JavaLongWrapper(l) }
        .map { wrapper =>
          val oldNumber: java.lang.Long = wrapper.number
          val primitiveValue: Long = 3L
          val additionalValue = wrapper.number + primitiveValue
          wrapper.add(oldNumber, additionalValue)
          wrapper
        }
      val plan2 = q2.queryExecution.executedPlan
      assertNoSerialization(plan2)
      q2.explain(true)
      checkDataset(q2, JavaLongWrapper(6L), JavaLongWrapper(9L))
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

  // todo accessing fields of nulls
  // todo If(IsNull(thisRef), NPEonNull(Literal(null, value.dataType)), value)

  test("null handling") {
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
      val ds = spark.range(1, 10).as[Long]

      def q1: Dataset[String] = ds
        .map(l => Event(l, null))
        .map(event => event.s.concat("suffix"))
      assertNoSerialization(q1.queryExecution.executedPlan)
      val thrownException1 = intercept[SparkException] {
        q1.collect()
      }
      assert(thrownException1.getCause.isInstanceOf[NullPointerException])

      def q2: Dataset[Event] = ds
        .map { l =>
          val e: Event = null
          e.doSmth()
          Event(l, l.toString)
        }
      // assert(plan2.schema == getValidSchema(q2))
      val thrownException2 = intercept[AnalysisException] {
        q2.collect()
      }
      assert(thrownException2.message == "Calling 'doSmth' on a null reference")

      def q3: Dataset[Event] = ds
        .map { l =>
          val e: Event = EventHelper.nullEvent()
          e.copy(l = l + 1)
        }
      // assert(plan3.schema == getValidSchema(q3))
      val thrownException3 = intercept[AnalysisException] {
        q3.collect()
      }
      assert(thrownException3.message == "Calling 'copy$default$2' on a null reference")

      def q4: Dataset[Event] = ds
        .map { l =>
          val e = Event(l, l.toString)
          val newE = e.generate()
          newE.copy(l = l + 1)
        }
      // assert(plan4.schema == getValidSchema(q4))
      val thrownException4 = intercept[AnalysisException] {
        q4.collect()
      }
      assert(thrownException4.message == "Calling 'copy$default$2' on a null reference")

      // TODO this one is currently broken
      def q5: Dataset[Event] = ds
        .map { l =>
          if (l % 2 == 0) ItemWrapper(l.toString, Item(l)) else ItemWrapper(l.toString, null)
        }
        .map { iw => Event(iw.item.i, iw.name) }
      assertNoSerialization(q5.queryExecution.executedPlan)
      val thrownException5 = intercept[SparkException] {
        q5.collect()
      }
      assert(thrownException5.getCause.isInstanceOf[NullPointerException])
    }
  }

  private def assertNoSerialization(plan: SparkPlan): Unit = {
    val isDeserializationAbsent = plan.collect { case d: DeserializeToObjectExec => d }.isEmpty
    assert(isDeserializationAbsent)
    val isSerializationAbsent = plan.collect { case s: SerializeFromObjectExec => s }.isEmpty
    assert(isSerializationAbsent)
  }

  private def getValidSchema(ds: => Dataset[_]): StructType = {
    var schema: StructType = null
    withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
      schema = ds.queryExecution.executedPlan.schema
    }
    schema
  }
}
