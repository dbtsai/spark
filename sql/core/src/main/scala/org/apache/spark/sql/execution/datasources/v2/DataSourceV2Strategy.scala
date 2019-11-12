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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.{AlterNamespaceSetProperties, AlterTable, AppendData, CreateNamespace, CreateTableAsSelect, CreateV2Table, DeleteFromTable, DescribeNamespace, DescribeTable, DropNamespace, DropTable, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, RefreshTable, RenameTable, Repartition, ReplaceTable, ReplaceTableAsSelect, SetCatalogAndNamespace, ShowCurrentNamespace, ShowNamespaces, ShowTableProperties, ShowTables}
import org.apache.spark.sql.connector.catalog.{StagingTableCatalog, TableCapability}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.v2.FilterV2
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

object DataSourceV2Strategy extends Strategy with PredicateHelper {

  import DataSourceV2Implicits._

  /**
   * Tries to convert an [[Expression]] that can be pushed down to a [[NamedReference]]
   */
  private def toNamedRef(e: Expression): Option[NamedReference] = {
    def helper(e: Expression): Option[Seq[String]] = e match {
      case a: Attribute =>
        Some(Seq(a.name))
      case s: GetStructField =>
        helper(s.child).map(_ ++ Seq(s.childSchema(s.ordinal).name))
      case _ =>
        None
    }
    helper(e).map(fieldNames => FieldReference(fieldNames))
  }

  private def translateLeafNodeFilterV2(predicate: Expression): Option[FilterV2] = predicate match {
    case expressions.EqualTo(e: Expression, Literal(v, t)) =>
      toNamedRef(e).map(field => sources.v2.EqualTo(field, convertToScala(v, t)))
    case expressions.EqualTo(Literal(v, t), e: Expression) =>
      toNamedRef(e).map(field => sources.v2.EqualTo(field, convertToScala(v, t)))

    case expressions.EqualNullSafe(e: Expression, Literal(v, t)) =>
      toNamedRef(e).map(field => sources.v2.EqualNullSafe(field, convertToScala(v, t)))
    case expressions.EqualNullSafe(Literal(v, t), e: Expression) =>
      toNamedRef(e).map(field => sources.v2.EqualNullSafe(field, convertToScala(v, t)))

    case expressions.GreaterThan(e: Expression, Literal(v, t)) =>
      toNamedRef(e).map(field => sources.v2.GreaterThan(field, convertToScala(v, t)))
    case expressions.GreaterThan(Literal(v, t), e: Expression) =>
      toNamedRef(e).map(field => sources.v2.LessThan(field, convertToScala(v, t)))

    case expressions.LessThan(e: Expression, Literal(v, t)) =>
      toNamedRef(e).map(field => sources.v2.LessThan(field, convertToScala(v, t)))
    case expressions.LessThan(Literal(v, t), e: Expression) =>
      toNamedRef(e).map(field => sources.v2.GreaterThan(field, convertToScala(v, t)))

    case expressions.GreaterThanOrEqual(e: Expression, Literal(v, t)) =>
      toNamedRef(e).map(field => sources.v2.GreaterThanOrEqual(field, convertToScala(v, t)))
    case expressions.GreaterThanOrEqual(Literal(v, t), e: Expression) =>
      toNamedRef(e).map(field => sources.v2.LessThanOrEqual(field, convertToScala(v, t)))

    case expressions.LessThanOrEqual(e: Expression, Literal(v, t)) =>
      toNamedRef(e).map(field => sources.v2.LessThanOrEqual(field, convertToScala(v, t)))
    case expressions.LessThanOrEqual(Literal(v, t), e: Expression) =>
      toNamedRef(e).map(field => sources.v2.GreaterThanOrEqual(field, convertToScala(v, t)))

    case expressions.InSet(e: Expression, set) =>
      val toScala = CatalystTypeConverters.createToScalaConverter(e.dataType)
      toNamedRef(e).map(field => sources.v2.In(field, set.toArray.map(toScala)))

    // Because we only convert In to InSet in Optimizer when there are more than certain
    // items. So it is possible we still get an In expression here that needs to be pushed
    // down.
    case expressions.In(e: Expression, list) if list.forall(_.isInstanceOf[Literal]) =>
      val hSet = list.map(_.eval(EmptyRow))
      val toScala = CatalystTypeConverters.createToScalaConverter(e.dataType)
      toNamedRef(e).map(field => sources.v2.In(field, hSet.toArray.map(toScala)))

    case expressions.IsNull(e: Expression) =>
      toNamedRef(e).map(field => sources.v2.IsNull(field))
    case expressions.IsNotNull(e: Expression) =>
      toNamedRef(e).map(field => sources.v2.IsNotNull(field))
    case expressions.StartsWith(e: Expression, Literal(v: UTF8String, StringType)) =>
      toNamedRef(e).map(field => sources.v2.StringStartsWith(field, v.toString))

    case expressions.EndsWith(e: Expression, Literal(v: UTF8String, StringType)) =>
      toNamedRef(e).map(field => sources.v2.StringEndsWith(field, v.toString))

    case expressions.Contains(e: Expression, Literal(v: UTF8String, StringType)) =>
      toNamedRef(e).map(field => sources.v2.StringContains(field, v.toString))

    case expressions.Literal(true, BooleanType) =>
      Some(sources.v2.AlwaysTrue)

    case expressions.Literal(false, BooleanType) =>
      Some(sources.v2.AlwaysFalse)

    case _ => None
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[FilterV2]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilterV2(predicate: Expression): Option[FilterV2] = {
    translateFilterV2WithMapping(predicate, None)
  }
  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[FilterV2]].
   *
   * @param predicate The input [[Expression]] to be translated as [[FilterV2]]
   * @param translatedFilterToExpr An optional map from leaf node filter expressions to its
   *                               translated [[FilterV2]]. The map is used for rebuilding
   *                               [[Expression]] from [[FilterV2]].
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilterV2WithMapping(
      predicate: Expression,
      translatedFilterToExpr: Option[mutable.HashMap[sources.v2.FilterV2, Expression]])
    : Option[sources.v2.FilterV2] = {
    predicate match {
      case expressions.And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilterV2WithMapping(left, translatedFilterToExpr)
          rightFilter <- translateFilterV2WithMapping(right, translatedFilterToExpr)
        } yield sources.v2.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilterV2WithMapping(left, translatedFilterToExpr)
          rightFilter <- translateFilterV2WithMapping(right, translatedFilterToExpr)
        } yield sources.v2.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilterV2WithMapping(child, translatedFilterToExpr).map(sources.v2.Not)

      case other =>
        val filter = translateLeafNodeFilterV2(other)
        if (filter.isDefined && translatedFilterToExpr.isDefined) {
          translatedFilterToExpr.get(filter.get) = predicate
        }
        filter
    }
  }

  protected[sql] def rebuildExpressionFromFilter(
      filter: FilterV2,
      translatedFilterToExpr: mutable.HashMap[sources.v2.FilterV2, Expression]): Expression = {
    filter match {
      case sources.v2.And(left, right) =>
        expressions.And(rebuildExpressionFromFilter(left, translatedFilterToExpr),
          rebuildExpressionFromFilter(right, translatedFilterToExpr))
      case sources.v2.Or(left, right) =>
        expressions.Or(rebuildExpressionFromFilter(left, translatedFilterToExpr),
          rebuildExpressionFromFilter(right, translatedFilterToExpr))
      case sources.v2.Not(pred) =>
        expressions.Not(rebuildExpressionFromFilter(pred, translatedFilterToExpr))
      case other =>
        translatedFilterToExpr.getOrElse(other,
          throw new AnalysisException(
            s"Fail to rebuild expression: missing key $filter in `translatedFilterToExpr`"))
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val batchExec = BatchScanExec(relation.output, relation.scan)

      val filterCondition = filters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, batchExec)).getOrElse(batchExec)

      val withProjection = if (withFilter.output != project || !batchExec.supportsColumnar) {
        ProjectExec(project, withFilter)
      } else {
        withFilter
      }

      withProjection :: Nil

    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isDefined =>
      val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
      val scanExec = MicroBatchScanExec(
        r.output, r.scan, microBatchStream, r.startOffset.get, r.endOffset.get)

      val withProjection = if (scanExec.supportsColumnar) {
        scanExec
      } else {
        // Add a Project here to make sure we produce unsafe rows.
        ProjectExec(r.output, scanExec)
      }

      withProjection :: Nil

    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isEmpty =>
      val continuousStream = r.stream.asInstanceOf[ContinuousStream]
      val scanExec = ContinuousScanExec(r.output, r.scan, continuousStream, r.startOffset.get)

      val withProjection = if (scanExec.supportsColumnar) {
        scanExec
      } else {
        // Add a Project here to make sure we produce unsafe rows.
        ProjectExec(r.output, scanExec)
      }

      withProjection :: Nil

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case CreateV2Table(catalog, ident, schema, parts, props, ifNotExists) =>
      CreateTableExec(catalog, ident, schema, parts, props, ifNotExists) :: Nil

    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists) =>
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicCreateTableAsSelectExec(
            staging, ident, parts, query, planLater(query), props, writeOptions, ifNotExists) :: Nil
        case _ =>
          CreateTableAsSelectExec(
            catalog, ident, parts, query, planLater(query), props, writeOptions, ifNotExists) :: Nil
      }

    case RefreshTable(catalog, ident) =>
      RefreshTableExec(catalog, ident) :: Nil

    case ReplaceTable(catalog, ident, schema, parts, props, orCreate) =>
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableExec(staging, ident, schema, parts, props, orCreate = orCreate) :: Nil
        case _ =>
          ReplaceTableExec(catalog, ident, schema, parts, props, orCreate = orCreate) :: Nil
      }

    case ReplaceTableAsSelect(catalog, ident, parts, query, props, options, orCreate) =>
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableAsSelectExec(
            staging,
            ident,
            parts,
            query,
            planLater(query),
            props,
            writeOptions,
            orCreate = orCreate) :: Nil
        case _ =>
          ReplaceTableAsSelectExec(
            catalog,
            ident,
            parts,
            query,
            planLater(query),
            props,
            writeOptions,
            orCreate = orCreate) :: Nil
      }

    case AppendData(r: DataSourceV2Relation, query, writeOptions, _) =>
      r.table.asWritable match {
        case v1 if v1.supports(TableCapability.V1_BATCH_WRITE) =>
          AppendDataExecV1(v1, writeOptions.asOptions, query) :: Nil
        case v2 =>
          AppendDataExec(v2, writeOptions.asOptions, planLater(query)) :: Nil
      }

    case OverwriteByExpression(r: DataSourceV2Relation, deleteExpr, query, writeOptions, _) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).map {
        filter => translateFilterV2(deleteExpr).getOrElse(
          throw new AnalysisException(s"Cannot translate expression to source filter: $filter"))
      }.toArray
      r.table.asWritable match {
        case v1 if v1.supports(TableCapability.V1_BATCH_WRITE) =>
          OverwriteByExpressionExecV1(v1, filters, writeOptions.asOptions, query) :: Nil
        case v2 =>
          OverwriteByExpressionExec(v2, filters, writeOptions.asOptions, planLater(query)) :: Nil
      }

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, writeOptions, _) =>
      OverwritePartitionsDynamicExec(
        r.table.asWritable, writeOptions.asOptions, planLater(query)) :: Nil

    case DeleteFromTable(relation, condition) =>
      relation match {
        case DataSourceV2ScanRelation(table, _, output) =>
          if (condition.exists(SubqueryExpression.hasSubquery)) {
            throw new AnalysisException(
              s"Delete by condition with subquery is not supported: $condition")
          }
          // fail if any filter cannot be converted.
          // correctness depends on removing all matching data.
          val filters = DataSourceStrategy.normalizeExprs(condition.toSeq, output)
              .flatMap(splitConjunctivePredicates(_).map {
                f => translateFilterV2(f).getOrElse(
                  throw new AnalysisException(s"Exec update failed:" +
                      s" cannot translate expression to source filter: $f"))
              }).toArray
          DeleteFromTableExec(table.asDeletable, filters) :: Nil
        case _ =>
          throw new AnalysisException("DELETE is only supported with v2 tables.")
      }

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case Repartition(1, false, child) =>
      val isContinuous = child.find {
        case r: StreamingDataSourceV2Relation => r.stream.isInstanceOf[ContinuousStream]
        case _ => false
      }.isDefined

      if (isContinuous) {
        ContinuousCoalesceExec(1, planLater(child)) :: Nil
      } else {
        Nil
      }

    case desc @ DescribeNamespace(catalog, namespace, extended) =>
      DescribeNamespaceExec(desc.output, catalog, namespace, extended) :: Nil

    case desc @ DescribeTable(DataSourceV2Relation(table, _, _), isExtended) =>
      DescribeTableExec(desc.output, table, isExtended) :: Nil

    case DropTable(catalog, ident, ifExists) =>
      DropTableExec(catalog, ident, ifExists) :: Nil

    case AlterTable(catalog, ident, _, changes) =>
      AlterTableExec(catalog, ident, changes) :: Nil

    case RenameTable(catalog, oldIdent, newIdent) =>
      RenameTableExec(catalog, oldIdent, newIdent) :: Nil

    case AlterNamespaceSetProperties(catalog, namespace, properties) =>
      AlterNamespaceSetPropertiesExec(catalog, namespace, properties) :: Nil

    case CreateNamespace(catalog, namespace, ifNotExists, properties) =>
      CreateNamespaceExec(catalog, namespace, ifNotExists, properties) :: Nil

    case DropNamespace(catalog, namespace, ifExists, cascade) =>
      DropNamespaceExec(catalog, namespace, ifExists, cascade) :: Nil

    case r: ShowNamespaces =>
      ShowNamespacesExec(r.output, r.catalog, r.namespace, r.pattern) :: Nil

    case r : ShowTables =>
      ShowTablesExec(r.output, r.catalog, r.namespace, r.pattern) :: Nil

    case SetCatalogAndNamespace(catalogManager, catalogName, namespace) =>
      SetCatalogAndNamespaceExec(catalogManager, catalogName, namespace) :: Nil

    case r: ShowCurrentNamespace =>
      ShowCurrentNamespaceExec(r.output, r.catalogManager) :: Nil

    case r @ ShowTableProperties(DataSourceV2Relation(table, _, _), propertyKey) =>
      ShowTablePropertiesExec(r.output, table, propertyKey) :: Nil

    case _ => Nil
  }
}
