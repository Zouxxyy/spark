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

package org.apache.spark.sql.learn.optimizer.emr

import org.apache.spark.sql.learn.BaseTest

class MergeOutputFile extends BaseTest {

  test("EMR Optimizer: Merge Output File with parquet") {
    sql("use spark_catalog")
    sql(s"CREATE DATABASE test_db LOCATION '$tempDBDir'")
    sql(s"USE test_db")
    sql(
      s"""
         |CREATE TABLE t1 (id INT) USING parquet
         |""".stripMargin)

    val numRows = 10000
    spark.range(1, numRows + 1)
      .toDF("id")
      .createOrReplaceTempView("source_tbl")

    // only when spark.sql.optimizer.plannedWrite.enabled is false, mergeOutputFiles can be valid.
    withSQLConf(
      "spark.sql.adaptive.mergeOutputFiles.enabled" -> "true",
      "spark.sql.optimizer.plannedWrite.enabled" -> "false") {
      val df = sql(
        s"""
           |INSERT INTO t1 SELECT * FROM source_tbl
           |""".stripMargin)

      printPlan(df)
    }

    /**
     * spark.sql.adaptive.mergeOutputFiles.enabled = false
     * === Optimized Plan ===
     * CommandResult Execute InsertIntoHadoopFsRelationCommand file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1, false, Parquet, [path=file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1], Append, `spark_catalog`.`test_db`.`t1`, org.apache.spark.sql.execution.datasources.InMemoryFileIndex(file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1), [id]
     * +- InsertIntoHadoopFsRelationCommand file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1, false, Parquet, [path=file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1], Append, `spark_catalog`.`test_db`.`t1`, org.apache.spark.sql.execution.datasources.InMemoryFileIndex(file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1), [id]
     * +- Project [cast(id#2L as int) AS id#5]
     * +- Project [id#2L]
     * +- SubqueryAlias source_tbl
     * +- View (`source_tbl`, [id#2L])
     * +- Project [id#0L AS id#2L]
     * +- Range (1, 10001, step=1, splits=Some(2))
     *
     *
     * === Executed Plan ===
     * CommandResult <empty>
     * +- Execute InsertIntoHadoopFsRelationCommand file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1, false, Parquet, [path=file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1], Append, `spark_catalog`.`test_db`.`t1`, org.apache.spark.sql.execution.datasources.InMemoryFileIndex(file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-7996efbf-a7ce-4cff-a866-437e6a12ac72/t1), [id]
     * +- *(1) Project [cast(id#0L as int) AS id#5]
     * +- *(1) Range (1, 10001, step=1, splits=2)
     */

    /**
     * spark.sql.adaptive.mergeOutputFiles.enabled = true
     * === Optimized Plan ===
     * CommandResult AdaptiveSparkPlan isFinalPlan=true
     * +- InsertIntoHadoopFsRelationCommand file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1, false, Parquet, [path=file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1], Append, `spark_catalog`.`test_db`.`t1`, org.apache.spark.sql.execution.datasources.InMemoryFileIndex(file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1), [id]
     * +- Project [cast(id#2L as int) AS id#5]
     * +- Project [id#2L]
     * +- SubqueryAlias source_tbl
     * +- View (`source_tbl`, [id#2L])
     * +- Project [id#0L AS id#2L]
     * +- Range (1, 10001, step=1, splits=Some(2))
     *
     * === Executed Plan ===
     * CommandResult <empty>
     * +- AdaptiveSparkPlan isFinalPlan=true
     * +- == Final Plan ==
     * Execute InsertIntoHadoopFsRelationCommand file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1, false, Parquet, [path=file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1], Append, `spark_catalog`.`test_db`.`t1`, org.apache.spark.sql.execution.datasources.InMemoryFileIndex(file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1), [id]
     * +- AQEShuffleRead coalesced
     * +- ShuffleQueryStage 0
     * +- Exchange RoundRobinPartitioning(5), MERGE_SMALL_FILES, [plan_id=40]
     * +- *(1) Project [cast(id#0L as int) AS id#5]
     * +- *(1) Range (1, 10001, step=1, splits=2)
     * +- == Initial Plan ==
     * Execute InsertIntoHadoopFsRelationCommand file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1, false, Parquet, [path=file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1], Append, `spark_catalog`.`test_db`.`t1`, org.apache.spark.sql.execution.datasources.InMemoryFileIndex(file:/private/var/folders/px/y3gybll50ggctcjp2t4r2b500000gp/T/spark-d056691a-324d-4f51-924e-16082314df30/t1), [id]
     * +- Exchange RoundRobinPartitioning(5), MERGE_SMALL_FILES, [plan_id=33]
     * +- Project [cast(id#0L as int) AS id#5]
     * +- Range (1, 10001, step=1, splits=2)
     */

    sql(s"DROP TABLE t1")
    sql(s"DROP DATABASE test_db")
    sql(s"USE paimon")
  }
}
