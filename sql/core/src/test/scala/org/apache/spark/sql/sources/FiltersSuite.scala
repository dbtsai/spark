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

package org.apache.spark.sql.sources

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.sources.v2.FiltersV2Suite.ref

/**
 * Unit test suites for data source filters.
 */
class FiltersSuite extends SparkFunSuite {

  test("EqualTo references") {
    assert(EqualTo("a", "1").references.toSeq == Seq("a"))
    assert(EqualTo("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))

    // Testing v1 to v2 filter conversions
    assert(EqualTo("a", "1").toV2 == v2.EqualTo(ref("a"), "1"))
    assert(EqualTo("a", EqualTo("b", "2")).toV2 ==
      v2.EqualTo(ref("a"), v2.EqualTo(ref("b"), "2")))
  }

  test("EqualNullSafe references") {
    assert(EqualNullSafe("a", "1").references.toSeq == Seq("a"))
    assert(EqualNullSafe("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))

    // Testing v1 to v2 filter conversions
    assert(EqualNullSafe("a", "1").toV2 == v2.EqualNullSafe(ref("a"), "1"))
    assert(EqualNullSafe("a", EqualTo("b", "2")).toV2 ==
      v2.EqualNullSafe(ref("a"), v2.EqualTo(ref("b"), "2")))
  }

  test("GreaterThan references") {
    assert(GreaterThan("a", "1").references.toSeq == Seq("a"))
    assert(GreaterThan("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))

    // Testing v1 to v2 filter conversions
    assert(GreaterThan("a", "1").toV2 == v2.GreaterThan(ref("a"), "1"))
    assert(GreaterThan("a", EqualTo("b", "2")).toV2 ==
      v2.GreaterThan(ref("a"), v2.EqualTo(ref("b"), "2")))
  }

  test("GreaterThanOrEqual references") {
    assert(GreaterThanOrEqual("a", "1").references.toSeq == Seq("a"))
    assert(GreaterThanOrEqual("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))

    // Testing v1 to v2 filter conversions
    assert(GreaterThanOrEqual("a", "1").toV2 == v2.GreaterThanOrEqual(ref("a"), "1"))
    assert(GreaterThanOrEqual("a", EqualTo("b", "2")).toV2 ==
      v2.GreaterThanOrEqual(ref("a"), v2.EqualTo(ref("b"), "2")))
  }

  test("LessThan references") {
    assert(LessThan("a", "1").references.toSeq == Seq("a"))
    assert(LessThan("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))

    // Testing v1 to v2 filter conversions
    assert(LessThan("a", "1").toV2 == v2.LessThan(ref("a"), "1"))
    assert(LessThan("a", EqualTo("b", "2")).toV2 ==
      v2.LessThan(ref("a"), v2.EqualTo(ref("b"), "2")))
  }

  test("LessThanOrEqual references") {
    assert(LessThanOrEqual("a", "1").references.toSeq == Seq("a"))
    assert(LessThanOrEqual("a", EqualTo("b", "2")).references.toSeq == Seq("a", "b"))
  }

  test("In references") {
    assert(In("a", Array("1")).references.toSeq == Seq("a"))
    assert(In("a", Array("1", EqualTo("b", "2"))).references.toSeq == Seq("a", "b"))
  }

  test("IsNull references") {
    assert(IsNull("a").references.toSeq == Seq("a"))
  }

  test("IsNotNull references") {
    assert(IsNotNull("a").references.toSeq == Seq("a"))
  }

  test("And references") {
    assert(And(EqualTo("a", "1"), EqualTo("b", "1")).references.toSeq == Seq("a", "b"))
  }

  test("Or references") {
    assert(Or(EqualTo("a", "1"), EqualTo("b", "1")).references.toSeq == Seq("a", "b"))
  }

  test("StringStartsWith references") {
    assert(StringStartsWith("a", "str").references.toSeq == Seq("a"))
  }

  test("StringEndsWith references") {
    assert(StringEndsWith("a", "str").references.toSeq == Seq("a"))
  }

  test("StringContains references") {
    assert(StringContains("a", "str").references.toSeq == Seq("a"))
  }
}
