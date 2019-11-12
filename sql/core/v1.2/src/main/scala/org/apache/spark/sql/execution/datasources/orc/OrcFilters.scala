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

package org.apache.spark.sql.execution.datasources.orc

import scala.reflect.ClassTag

import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgument}
import org.apache.orc.storage.ql.io.sarg.SearchArgument.Builder
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory.newBuilder
import org.apache.orc.storage.serde2.io.HiveDecimalWritable

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.FilterV2
import org.apache.spark.sql.types._

/**
 * Helper object for building ORC `SearchArgument`s, which are used for ORC predicate push-down.
 *
 * Due to limitation of ORC `SearchArgument` builder, we had to implement separate checking and
 * conversion passes through the Filter to make sure we only convert predicates that are known
 * to be convertible.
 *
 * An ORC `SearchArgument` must be built in one pass using a single builder.  For example, you can't
 * build `a = 1` and `b = 2` first, and then combine them into `a = 1 AND b = 2`.  This is quite
 * different from the cases in Spark SQL or Parquet, where complex filters can be easily built using
 * existing simpler ones.
 *
 * The annoying part is that, `SearchArgument` builder methods like `startAnd()`, `startOr()`, and
 * `startNot()` mutate internal state of the builder instance.  This forces us to translate all
 * convertible filters with a single builder instance. However, if we try to translate a filter
 * before checking whether it can be converted or not, we may end up with a builder whose internal
 * state is inconsistent in the case of an inconvertible filter.
 *
 * For example, to convert an `And` filter with builder `b`, we call `b.startAnd()` first, and then
 * try to convert its children.  Say we convert `left` child successfully, but find that `right`
 * child is inconvertible.  Alas, `b.startAnd()` call can't be rolled back, and `b` is inconsistent
 * now.
 *
 * The workaround employed here is to trim the Spark filters before trying to convert them. This
 * way, we can only do the actual conversion on the part of the Filter that is known to be
 * convertible.
 *
 * P.S.: Hive seems to use `SearchArgument` together with `ExprNodeGenericFuncDesc` only.  Usage of
 * builder methods mentioned above can only be found in test code, where all tested filters are
 * known to be convertible.
 */
private[sql] object OrcFilters extends OrcFiltersBase {

  /**
   * Create ORC filter as a SearchArgument instance.
   */
  def createFilter(schema: StructType, filters: Seq[Filter])(implicit d: DummyImplicit)
      : Option[SearchArgument] = {
    createFilter(schema, filters.map(_.toV2))
  }

  def createFilter(schema: StructType, filters: Seq[FilterV2]): Option[SearchArgument] = {
    // TODO: Fix me for nested data
    val dataTypeMap: Map[NamedReference, DataType] =
      schema.map(f => FieldReference(Seq(f.name)) -> f.dataType).toMap
    // Combines all convertible filters using `And` to produce a single conjunction
    val conjunctionOptional = buildTree(convertibleFilters(schema, dataTypeMap, filters))
    conjunctionOptional.map { conjunction =>
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate.
      // The input predicate is fully convertible. There should not be any empty result in the
      // following recursive method call `buildSearchArgument`.
      buildSearchArgument(dataTypeMap, conjunction, newBuilder).build()
    }
  }

  def convertibleFilters(
      schema: StructType,
      dataTypeMap: Map[NamedReference, DataType],
      filters: Seq[FilterV2]): Seq[FilterV2] = {
    import org.apache.spark.sql.sources.v2._

    def convertibleFiltersHelper(
        filter: FilterV2,
        canPartialPushDown: Boolean): Option[FilterV2] = filter match {
      // At here, it is not safe to just convert one side and remove the other side
      // if we do not understand what the parent filters are.
      //
      // Here is an example used to explain the reason.
      // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
      // convert b in ('1'). If we only convert a = 2, we will end up with a filter
      // NOT(a = 2), which will generate wrong results.
      //
      // Pushing one side of AND down is only safe to do at the top level or in the child
      // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
      // can be safely removed.
      case And(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        (leftResultOptional, rightResultOptional) match {
          case (Some(leftResult), Some(rightResult)) => Some(And(leftResult, rightResult))
          case (Some(leftResult), None) if canPartialPushDown => Some(leftResult)
          case (None, Some(rightResult)) if canPartialPushDown => Some(rightResult)
          case _ => None
        }

      // The Or predicate is convertible when both of its children can be pushed down.
      // That is to say, if one/both of the children can be partially pushed down, the Or
      // predicate can be partially pushed down as well.
      //
      // Here is an example used to explain the reason.
      // Let's say we have
      // (a1 AND a2) OR (b1 AND b2),
      // a1 and b1 is convertible, while a2 and b2 is not.
      // The predicate can be converted as
      // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
      // As per the logical in And predicate, we can push down (a1 OR b1).
      case Or(left, right) =>
        for {
          lhs <- convertibleFiltersHelper(left, canPartialPushDown)
          rhs <- convertibleFiltersHelper(right, canPartialPushDown)
        } yield Or(lhs, rhs)
      case Not(pred) =>
        val childResultOptional = convertibleFiltersHelper(pred, canPartialPushDown = false)
        childResultOptional.map(Not)
      case other =>
        for (_ <- buildLeafSearchArgument(dataTypeMap, other, newBuilder())) yield other
    }
    filters.flatMap { filter =>
      convertibleFiltersHelper(filter, true)
    }
  }

  /**
   * Get PredicateLeafType which is corresponding to the given DataType.
   */
  private def getPredicateLeafType(dataType: DataType) = dataType match {
    case BooleanType => PredicateLeaf.Type.BOOLEAN
    case ByteType | ShortType | IntegerType | LongType => PredicateLeaf.Type.LONG
    case FloatType | DoubleType => PredicateLeaf.Type.FLOAT
    case StringType => PredicateLeaf.Type.STRING
    case DateType => PredicateLeaf.Type.DATE
    case TimestampType => PredicateLeaf.Type.TIMESTAMP
    case _: DecimalType => PredicateLeaf.Type.DECIMAL
    case _ => throw new UnsupportedOperationException(s"DataType: ${dataType.catalogString}")
  }

  /**
   * Cast literal values for filters.
   *
   * We need to cast to long because ORC raises exceptions
   * at 'checkLiteralType' of SearchArgumentImpl.java.
   */
  private def castLiteralValue(value: Any, dataType: DataType): Any = dataType match {
    case ByteType | ShortType | IntegerType | LongType =>
      value.asInstanceOf[Number].longValue
    case FloatType | DoubleType =>
      value.asInstanceOf[Number].doubleValue()
    case _: DecimalType =>
      new HiveDecimalWritable(HiveDecimal.create(value.asInstanceOf[java.math.BigDecimal]))
    case _ => value
  }

  /**
   * Build a SearchArgument and return the builder so far.
   *
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input predicates, which should be fully convertible to SearchArgument.
   * @param builder the input SearchArgument.Builder.
   * @return the builder so far.
   */
  private def buildSearchArgument(
      dataTypeMap: Map[NamedReference, DataType],
      expression: FilterV2,
      builder: Builder): Builder = {
    import org.apache.spark.sql.sources.v2._

    expression match {
      case And(left, right) =>
        val lhs = buildSearchArgument(dataTypeMap, left, builder.startAnd())
        val rhs = buildSearchArgument(dataTypeMap, right, lhs)
        rhs.end()

      case Or(left, right) =>
        val lhs = buildSearchArgument(dataTypeMap, left, builder.startOr())
        val rhs = buildSearchArgument(dataTypeMap, right, lhs)
        rhs.end()

      case Not(child) =>
        buildSearchArgument(dataTypeMap, child, builder.startNot()).end()

      case other =>
        buildLeafSearchArgument(dataTypeMap, other, builder).getOrElse {
          throw new SparkException(
            "The input filter of OrcFilters.buildSearchArgument should be fully convertible.")
        }
    }
  }

  /**
   * Build a SearchArgument for a leaf predicate and return the builder so far.
   *
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input filter predicates.
   * @param builder the input SearchArgument.Builder.
   * @return the builder so far.
   */
  private def buildLeafSearchArgument(
      dataTypeMap: Map[NamedReference, DataType],
      expression: FilterV2,
      builder: Builder): Option[Builder] = {
    def getType(field: NamedReference): PredicateLeaf.Type =
      getPredicateLeafType(dataTypeMap(field))

    import org.apache.spark.sql.sources.v2._

    // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
    // call is mandatory. ORC `SearchArgument` builder requires that all leaf predicates must be
    // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).
    expression match {
      case EqualTo(field, value) if isSearchableType(dataTypeMap(field)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(field))
        Some(builder.startAnd().equals(field.name(), getType(field), castedValue).end())

      case EqualNullSafe(field, value) if isSearchableType(dataTypeMap(field)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(field))
        Some(builder.startAnd().nullSafeEquals(field.name(), getType(field), castedValue).end())

      case LessThan(field, value) if isSearchableType(dataTypeMap(field)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(field))
        Some(builder.startAnd().lessThan(field.name(), getType(field), castedValue).end())

      case LessThanOrEqual(field, value) if isSearchableType(dataTypeMap(field)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(field))
        Some(builder.startAnd().lessThanEquals(field.name(), getType(field), castedValue).end())

      case GreaterThan(field, value) if isSearchableType(dataTypeMap(field)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(field))
        Some(builder.startNot().lessThanEquals(field.name(), getType(field), castedValue).end())

      case GreaterThanOrEqual(field, value) if isSearchableType(dataTypeMap(field)) =>
        val castedValue = castLiteralValue(value, dataTypeMap(field))
        Some(builder.startNot().lessThan(field.name(), getType(field), castedValue).end())

      case IsNull(field) if isSearchableType(dataTypeMap(field)) =>
        Some(builder.startAnd().isNull(field.name(), getType(field)).end())

      case IsNotNull(field) if isSearchableType(dataTypeMap(field)) =>
        Some(builder.startNot().isNull(field.name(), getType(field)).end())

      case In(field, values) if isSearchableType(dataTypeMap(field)) =>
        val castedValues = values.map(v => castLiteralValue(v, dataTypeMap(field)))
        Some(builder.startAnd().in(field.name(), getType(field),
          castedValues.map(_.asInstanceOf[AnyRef]): _*).end())

      case _ => None
    }
  }
}
