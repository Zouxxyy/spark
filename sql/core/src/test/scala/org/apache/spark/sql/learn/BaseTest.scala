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

package org.apache.spark.sql.learn

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

abstract class BaseTest extends QueryTest with SharedSparkSession {

  val _spark: SparkSession = spark

  protected lazy val tempDBDir: File = Utils.createTempDir()

  protected val dbName0: String = "test"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.paimon.warehouse", tempDBDir.getCanonicalPath)
      .set("spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.planChangeLog.level", "error")
      .set("spark.sql.planChangeLog.batches", "")
      .set("spark.sql.planChangeLog.rules", "")
//      .set("spark.default.parallelism", "4")
      .set("spark.eventLog.enabled", "false")
      .set("spark.eventLog.dir", "/Users/zxy/data/spark/history")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"USE paimon")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS paimon.$dbName0")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"USE paimon")
      // spark.sql(s"USE default")
      spark.sql(s"DROP DATABASE IF EXISTS paimon.$dbName0 CASCADE")
    } finally {
      super.afterAll()
    }
  }

  def printPlan(df: DataFrame): Unit = {
    // scalastyle:off println
    println("=== Optimized Plan ===")
    val optimizedPlan = df.queryExecution.optimizedPlan
    println(optimizedPlan)

    println("=== Executed Plan ===")
    val executedPlan = df.queryExecution.executedPlan
    println(executedPlan)
    // scalastyle:on println

    df.show()
  }
}
