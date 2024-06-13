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

import org.apache.spark.sql.learn.BaseTest

class Filter extends BaseTest {

  test("Filter: test partition filter") {
    sql("use spark_catalog")
    sql(s"CREATE DATABASE test_db LOCATION '$tempDBDir'")
    sql(s"USE test_db")

    withTable("t") {
      sql(
        s"""
           |CREATE TABLE test_db.t (id INT, p STRING)
           |USING parquet
           |PARTITIONED BY (p)
           |""".stripMargin)
      sql("INSERT INTO test_db.t VALUES (1, '1')")

      printPlan(sql("SELECT * FROM test_db.t WHERE p = 1"))
    }

    withTable("t") {
      sql(
        s"""
           |CREATE TABLE test_db.t (id INT, p INT)
           |USING parquet
           |PARTITIONED BY (p)
           |""".stripMargin)
      sql("INSERT INTO test_db.t VALUES (1, 1)")

      printPlan(sql("SELECT * FROM test_db.t WHERE p = '1'"))
    }
    sql(s"DROP DATABASE test_db")
    sql(s"USE paimon")
  }
}
