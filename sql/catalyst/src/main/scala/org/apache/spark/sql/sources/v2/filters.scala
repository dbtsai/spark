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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.connector.expressions.NamedReference

/**
 * A filter that evaluates to `true` iff the field evaluates to a value
 * equal to `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class EqualTo(ref: NamedReference, value: Any) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref) ++ findReferences(value)
}

/**
 * Performs equality comparison, similar to [[EqualTo]]. However, this differs from [[EqualTo]]
 * in that it returns `true` (rather than NULL) if both inputs are NULL, and `false`
 * (rather than NULL) if one of the input is NULL and the other is not NULL.
 *
 * @since 3.0.0
 */
@Experimental
case class EqualNullSafe(ref: NamedReference, value: Any) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref) ++ findReferences(value)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to a value
 * greater than `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class GreaterThan(ref: NamedReference, value: Any) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref) ++ findReferences(value)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to a value
 * greater than or equal to `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class GreaterThanOrEqual(ref: NamedReference, value: Any) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref) ++ findReferences(value)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to a value
 * less than `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class LessThan(ref: NamedReference, value: Any) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref) ++ findReferences(value)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to a value
 * less than or equal to `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class LessThanOrEqual(ref: NamedReference, value: Any) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref) ++ findReferences(value)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to one of the values in the array.
 *
 * @since 3.0.0
 */
@Experimental
case class In(ref: NamedReference, values: Array[Any]) extends FilterV2 {
  override def hashCode(): Int = {
    var h = ref.hashCode
    values.foreach { v =>
      h *= 41
      h += v.hashCode()
    }
    h
  }
  override def equals(o: Any): Boolean = o match {
    case In(a, vs) =>
      a == ref && vs.length == values.length && vs.zip(values).forall(x => x._1 == x._2)
    case _ => false
  }
  override def toString: String = {
    s"In($ref, [${values.mkString(",")}])"
  }

  override def references: Array[NamedReference] = Array(ref) ++ values.flatMap(findReferences)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to null.
 *
 * @since 3.0.0
 */
@Experimental
case class IsNull(ref: NamedReference) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to a non-null value.
 *
 * @since 3.0.0
 */
@Experimental
case class IsNotNull(ref: NamedReference) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref)
}

/**
 * A filter that evaluates to `true` iff both `left` or `right` evaluate to `true`.
 *
 * @since 3.0.0
 */
@Experimental
case class And(left: FilterV2, right: FilterV2) extends FilterV2 {
  override def references: Array[NamedReference] = left.references ++ right.references
}

/**
 * A filter that evaluates to `true` iff at least one of `left` or `right` evaluates to `true`.
 *
 * @since 3.0.0
 */
@Experimental
case class Or(left: FilterV2, right: FilterV2) extends FilterV2 {
  override def references: Array[NamedReference] = left.references ++ right.references
}

/**
 * A filter that evaluates to `true` iff `child` is evaluated to `false`.
 *
 * @since 3.0.0
 */
@Experimental
case class Not(child: FilterV2) extends FilterV2 {
  override def references: Array[NamedReference] = child.references()
}

/**
 * A filter that evaluates to `true` iff the field evaluates to
 * a string that starts with `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class StringStartsWith(ref: NamedReference, value: String) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to
 * a string that ends with `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class StringEndsWith(ref: NamedReference, value: String) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref)
}

/**
 * A filter that evaluates to `true` iff the field evaluates to
 * a string that contains the string `value`.
 *
 * @since 3.0.0
 */
@Experimental
case class StringContains(ref: NamedReference, value: String) extends FilterV2 {
  override def references: Array[NamedReference] = Array(ref)
}

/**
 * A filter that always evaluates to `true`.
 */
@Experimental
case class AlwaysTrue() extends FilterV2 {
  override def references: Array[NamedReference] = Array.empty
}

@Experimental
object AlwaysTrue extends AlwaysTrue {
}

/**
 * A filter that always evaluates to `false`.
 */
@Experimental
case class AlwaysFalse() extends FilterV2 {
  override def references: Array[NamedReference] = Array.empty
}

@Experimental
object AlwaysFalse extends AlwaysFalse {
}
