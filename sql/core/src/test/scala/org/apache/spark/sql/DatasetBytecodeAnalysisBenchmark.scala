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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

object DatasetBytecodeAnalysisBenchmark extends SqlBasedBenchmark {

  case class Data(l: Long, s: String)
  case class Event(userId: Long, deviceId: String, counts: Long) {
    def doSmth(): Boolean = true
  }

  def backToBackMapLong(numRows: Long, minNumIters: Int, explain: Boolean): Benchmark = {
    import spark.implicits._

    val benchmarkName = "back-to-back maps (longs)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)
    val ds = spark.range(1, numRows)
    val df = ds.toDF("l")

    benchmark.addCase("DataFrame") { _ =>
      val res = df
        .select($"l" + 1 as "l")
        .select($"l" + 2 as "l")
        .select($"l" + 3 as "l")
        .select($"l" + 4 as "l")
        .select($"l" + 5 as "l")
        .select($"l" + 6 as "l")

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    val datasetTest = () => {
      val res = ds.as[Long]
        .map(l => l + 1)
        .map(l => l + 2)
        .map(l => l + 3)
        .map(l => l + 4)
        .map(l => l + 5)
        .map(l => l + 6)

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("No bytecode analysis") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetTest()
      }
    }

    benchmark.addCase("Bytecode analysis enabled") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetTest()
      }
    }

    benchmark
  }

  def filterMapFilterLong(numRows: Long, minNumIters: Int, explain: Boolean): Benchmark = {
    import spark.implicits._

    val benchmarkName = "filter -> map -> filter (longs)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)

    val ds = spark.range(1, numRows)
    val df = ds.toDF("l")

    benchmark.addCase("DataFrame") { _ =>
      val res = df
        .filter($"l" >= 5000000)
        .select($"l" + 1 as "l")
        .filter($"l" <= 5000001)

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    val datasetTest = () => {
      val res = ds.as[Long]
        .filter(l => l >= 5000000)
        .map(l => l + 1)
        .filter(l => l <= 5000001)

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("No bytecode analysis") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetTest()
      }
    }

    benchmark.addCase("Bytecode analysis enabled") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetTest()
      }
    }

    benchmark
  }

  def aggregationLong(numRows: Long, minNumIters: Int, explain: Boolean): Benchmark = {
    import spark.implicits._

    val benchmarkName = "aggregation (longs)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)

    val ds = spark.range(1, numRows)
    val df = ds.toDF("l")

    benchmark.addCase("DataFrame") { _ =>
      val res = df
        .select($"l" + 1 as "l")
        .select($"l" + 2 as "l")
        .select(sum($"l"))

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    val datasetTest = () => {
      val res = spark.range(1, numRows).as[Long]
        .map(l => l + 1)
        .map(l => l + 2)
        .select(typed.sumLong((l: Long) => l))

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("No bytecode analysis") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetTest()
      }
    }

    benchmark.addCase("Bytecode analysis enabled") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetTest()
      }
    }

    benchmark
  }

  def trivialMapCaseClasses(numRows: Long, minNumIters: Int, explain: Boolean): Benchmark = {
    import spark.implicits._

    val benchmarkName = "trivial map (case classes)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)

    val datasetTest = () => {
      val res = spark.range(1, numRows).map(n => Data(n, n.toString)).as[Data]
        .map(_.l)
        .map(l => l + 13)
        .map(l => l + 22)

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("No bytecode analysis") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetTest()
      }
    }

    benchmark.addCase("Bytecode analysis enabled") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetTest()
      }
    }

    benchmark
  }

  def mapBetweenCaseClasses(numRows: Long, minNumIters: Int, explain: Boolean): Benchmark = {
    import spark.implicits._

    val benchmarkName = "map (case classes)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)

    val datasetTest = () => {
      val res = spark.range(1, numRows).map(n => Data(n, n.toString)).as[Data]
        .map(_.l)
        .map(l => l + 13)
        .map(l => Data(l, l.toString))
        .map(data => Event(data.l, data.s, data.l))

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("No bytecode analysis") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetTest()
      }
    }

    benchmark.addCase("Bytecode analysis enabled") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetTest()
      }
    }

    benchmark
  }

  def mapAndFilterCaseClasses(numRows: Long, minNumIters: Int, explain: Boolean): Benchmark = {
    import spark.implicits._

    val benchmarkName = "map -> filter (case classes)"
    val benchmark = new Benchmark(benchmarkName, numRows, minNumIters)

    // TODO map(funcs(0)) failed with Scala 2.12, check if it even worked correctly for 2.11
    // val func = (l: Long, i: Long) => Event(l, l.toString, i)
    // val funcs = 0.until(2).map { i =>
    //   (l: Long) => func(l, i)
    // }

    // use func directly for now
    val func = (l: Long) => Event(l, l.toString, 0)

    val datasetTest = () => {
      val res = spark.range(1, numRows).as[Long]
        .map(func)
        .map(event => Event(event.userId, event.deviceId, event.counts + 1))
        .filter(_.userId < 10)

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("No bytecode analysis") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
        datasetTest()
      }
    }

    benchmark.addCase("Bytecode analysis enabled") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetTest()
      }
    }

    benchmark
  }

  def returnNullEvent(): Event = null

  def nullHandling(numRows: Long, explain: Boolean): Benchmark = {
    import spark.implicits._

    val benchmark = new Benchmark("null handling", numRows, 2)

    val datasetTest = () => {
      val res = spark.range(1, numRows).map(n => Event(n, null, n)).as[Event]
        .map{ event =>
          val e: Event = returnNullEvent()
          val r = e.doSmth()
          event.deviceId.concat("suffix")
        }

      if (explain) res.explain(true)
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    // benchmark.addCase("No bytecode analysis") { _ =>
    //   withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "false") {
    //     datasetTest()
    //   }
    // }

    benchmark.addCase("Bytecode analysis enabled") { _ =>
      withSQLConf(SQLConf.BYTECODE_ANALYSIS_ENABLED.key -> "true") {
        datasetTest()
      }
    }

    benchmark
  }

  // TODO nested data types

  // TODO undeterministic

  override def getSparkSession: SparkSession = {
    SparkSession.builder
      .master("local[*]")
      .appName("Dataset bytecode analysis benchmark")
      .getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows = 100000000
    val explain = false

    backToBackMapLong(numRows, minNumIters = 1, explain).run()
    filterMapFilterLong(numRows, minNumIters = 1, explain).run()
    aggregationLong(numRows, minNumIters = 1, explain).run()
    trivialMapCaseClasses(numRows, minNumIters = 1, explain).run()
    mapBetweenCaseClasses(numRows, minNumIters = 1, explain).run()
    mapAndFilterCaseClasses(numRows, minNumIters = 1, explain).run()
    nullHandling(numRows, explain).run()
  }

}
