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

package org.apache.spark.sql.execution.datasources.v2.parquet

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.v2.FilterV2
import org.apache.spark.sql.sources.v2.reader.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ParquetScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsPushDownFilters {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  lazy val pushedParquetFilters = {
    val sqlConf = sparkSession.sessionState.conf
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetSchema =
      new SparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(schema)
    val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
    // parquetFilters.convertibleFilters(this.filters).toArray
    this.filters
  }

  private var filters: Array[FilterV2] = Array.empty

  override def pushFilters(filters: Array[FilterV2]): Array[FilterV2] = {
    this.filters = filters
    this.filters
  }

  // Note: for Parquet, the actual filter push down happens in [[ParquetPartitionReaderFactory]].
  // It requires the Parquet physical schema to determine whether a filter is convertible.
  // All filters that can be converted to Parquet are pushed down.
  override def pushedFilters(): Array[FilterV2] = pushedParquetFilters

  override def build(): Scan = {
    ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
      readPartitionSchema(), pushedParquetFilters, options)
  }
}
