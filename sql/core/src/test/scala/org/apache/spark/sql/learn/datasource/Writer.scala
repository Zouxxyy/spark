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

package org.apache.spark.sql.learn.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{expr, rand}
import org.apache.spark.sql.learn.BaseTest

class Writer extends BaseTest {

  test("Writer: paimon writer") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (id INT, name STRING, pt STRING)
           |USING paimon
           |tblproperties ('file.format' = 'parquet', 'bucket' = '2', 'bucket-key' = 'id')
           |PARTITIONED BY (pt)
           |""".stripMargin)

      val df = sql(
        s"""INSERT INTO t1 VALUES
           |(1, 'x', 'p1'),
           |(2, 'y', 'p2'),
           |(3, 'y', 'p3')
           |""".stripMargin)

      printPlan(df)

      checkAnswer(
        sql(s"SELECT * FROM t1"),
        Seq(
          Row(1, "x", "p1"),
          Row(2, "y", "p2"),
          Row(3, "y", "p3")
        )
      )
    }
  }

  test("Writer: parquet writer") {
    sql("use spark_catalog")
    sql(s"CREATE DATABASE test_db LOCATION '$tempDBDir'")
    sql(s"USE test_db")

    sql(
      s"""
         |CREATE TABLE test_db.target_tbl (id INT, name INT, pt STRING)
         |USING parquet
         |PARTITIONED BY (pt)
         |""".stripMargin)

    val numRows = 10000
    spark.range(1, numRows + 1)
      .toDF("id")
      .withColumn("name", (rand() * 100).cast("int")) // random [0, 99]
      .withColumn("pt", expr("concat('p', cast(floor(rand() * 8 + 1) as int))")) // random ["p1", "p8"]
      .withColumn("random", rand()).orderBy("random").drop("random")
      .createOrReplaceTempView("source_tbl")

     sql(s"SELECT id, name, pt FROM source_tbl").show(10)

    // case1
    sql(
      s"""
         |INSERT INTO TABLE target_tbl
         |SELECT id, name, pt FROM source_tbl
         |DISTRIBUTE BY pt SORT BY pt, id
         |""".stripMargin)

    // case2
//    withSQLConf(
//      "spark.sql.optimizer.plannedWrite.enabled" -> "false"
//    ) {
//      sql(
//        s"""
//           |INSERT INTO TABLE target_tbl
//           |SELECT id, name, pt FROM source_tbl
//           |DISTRIBUTE BY pt SORT BY id
//           |""".stripMargin)
//    }

    // case3
//    withSQLConf(
//      "spark.sql.maxConcurrentOutputFileWriters" -> "1",
//    ) {
//      sql(
//        s"""
//           |INSERT INTO TABLE target_tbl
//           |SELECT id, name, pt FROM source_tbl
//           |DISTRIBUTE BY pt SORT BY id
//           |""".stripMargin)
//    }

    // case4
//    withSQLConf(
//      "spark.sql.maxConcurrentOutputFileWriters" -> "2",
//    ) {
//      sql(
//        s"""
//           |INSERT INTO TABLE target_tbl
//           |SELECT id, name, pt FROM source_tbl
//           |DISTRIBUTE BY pt SORT BY id
//           |""".stripMargin)
//    }

    // case 5
//    withSQLConf(
//      "spark.sql.maxConcurrentOutputFileWriters" -> "2",
//    ) {
//      sql(
//        s"""
//           |INSERT INTO TABLE target_tbl
//           |SELECT id, name, pt FROM source_tbl
//           |DISTRIBUTE BY (pt, case when id < ${numRows / 2} then 1 else 2 end) SORT BY id
//           |""".stripMargin)
//    }

    // case 6
//    withSQLConf(
//      "spark.sql.maxConcurrentOutputFileWriters" -> "2",
//    ) {
//      sql(
//        s"""
//           |INSERT INTO TABLE target_tbl
//           |SELECT id, name, pt FROM source_tbl
//           |DISTRIBUTE BY
//           |case when id is not null then id else id % 60 end,
//           |case when id < ${numRows / 2} then 1 else 2 end
//           |SORT BY id
//           |""".stripMargin)
//    }

    sql(s"SELECT * FROM target_tbl").show(10)

    sql(s"DROP TABLE target_tbl")
    sql(s"DROP DATABASE test_db")
    sql(s"USE paimon")
  }
}
