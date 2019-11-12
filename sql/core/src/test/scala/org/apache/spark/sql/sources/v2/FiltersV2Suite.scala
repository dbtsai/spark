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

package org.apache.spark.sql.sources.v2

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.sources.v2.FiltersV2Suite.ref

class FiltersV2Suite extends SparkFunSuite {

  test("References with nested columns") {
    assert(EqualTo(ref("a", "B"), "1").references.map(_.describe()).toSeq == Seq("a.B"))
    assert(EqualTo(ref("a", "b.c"), "1").references.map(_.describe()).toSeq == Seq("a.`b.c`"))
    assert(EqualTo(ref("`a`.b", "c"), "1").references.map(_.describe()).toSeq == Seq("```a``.b`.c"))
  }

  test("EqualTo references") {
    assert(EqualTo(ref("a"), "1").references.map(_.describe()).toSeq == Seq("a"))
    assert(EqualTo(ref("a"), EqualTo(ref("b"), "2")).references.map(_.describe()).toSeq ==
      Seq("a", "b"))
  }

  test("EqualNullSafe references") {
    assert(EqualNullSafe(ref("a"), "1").references.map(_.describe()).toSeq == Seq("a"))
    assert(EqualNullSafe(ref("a"), EqualTo(ref("b"), "2")).references.map(_.describe()).toSeq ==
      Seq("a", "b"))
  }

  test("GreaterThan references") {
    assert(GreaterThan(ref("a"), "1").references.map(_.describe()).toSeq == Seq("a"))
    assert(GreaterThan(ref("a"), EqualTo(ref("b"), "2")).references.map(_.describe()).toSeq
      == Seq("a", "b"))
  }

  test("GreaterThanOrEqual references") {
    assert(GreaterThanOrEqual(ref("a"), "1").references.map(_.describe()).toSeq == Seq("a"))
    assert(GreaterThanOrEqual(ref("a"), EqualTo(ref("b"), "2")).references.map(_.describe()).toSeq
      == Seq("a", "b"))
  }

  test("LessThan references") {
    assert(LessThan(ref("a"), "1").references.map(_.describe()).toSeq ==
      Seq("a"))
    assert(LessThan(ref("a"), EqualTo(ref("b"), "2")).references.map(_.describe()).toSeq ==
      Seq("a", "b"))
  }

  test("LessThanOrEqual references") {
    assert(LessThanOrEqual(ref("a"), "1").references.map(_.describe()).toSeq ==
      Seq("a"))
    assert(LessThanOrEqual(ref("a"), EqualTo(ref("b"), "2")).references.map(_.describe()).toSeq ==
      Seq("a", "b"))
  }

  test("In references") {
    assert(In(ref("a"), Array("1")).references.map(_.describe()).toSeq == Seq("a"))
    assert(In(ref("a"), Array("1", EqualTo(ref("b"), "2"))).references.map(_.describe()).toSeq
      == Seq("a", "b"))
  }

  test("IsNull references") {
    assert(IsNull(ref("a")).references.map(_.describe()).toSeq
      == Seq("a"))
  }

  test("IsNotNull references") {
    assert(IsNotNull(ref("a")).references.map(_.describe()).toSeq
      == Seq("a"))
  }

  test("And references") {
    assert(And(EqualTo(ref("a"), "1"), EqualTo(ref("b"), "1")).references.map(_.describe()).toSeq ==
      Seq("a", "b"))
  }

  test("Or references") {
    assert(Or(EqualTo(ref("a"), "1"), EqualTo(ref("b"), "1")).references.map(_.describe()).toSeq ==
      Seq("a", "b"))
  }

  test("StringStartsWith references") {
    assert(StringStartsWith(ref("a"), "str").references.map(_.describe()).toSeq == Seq("a"))
  }

  test("StringEndsWith references") {
    assert(StringEndsWith(ref("a"), "str").references.map(_.describe()).toSeq == Seq("a"))
  }

  test("StringContains references") {
    assert(StringContains(ref("a"), "str").references.map(_.describe()).toSeq == Seq("a"))
  }
}

object FiltersV2Suite {
  private[sql] def ref(parts: String*): FieldReference = {
    new FieldReference(parts)
  }
}
