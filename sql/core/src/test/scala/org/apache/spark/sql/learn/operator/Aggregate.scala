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

package org.apache.spark.sql.learn.operator

import org.apache.spark.sql.learn.BaseTest

class Aggregate extends BaseTest {

  test(s"Aggregate: Distinct") {
    withTable("t") {
      spark.sql(s"""
                   |CREATE TABLE t (id INT, name String, value INT)
                   |USING paimon
                   |tblproperties ('file.format' = 'parquet')
                   |""".stripMargin)

      spark.sql(
        s"""INSERT INTO t VALUES
           |(1, 'a', 1), (1, 'a', 2), (1, 'c', 3),
           |(2, 'd', 1), (2, 'e', 2),
           |(3, 'g', 1), (3, 'h', 2), (3, 'h', 3), (3, 'i', 4)
           |""".stripMargin)

      withSQLConf(
        "spark.sql.planChangeLog.rules" ->
          "org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate") {
        val df1 = spark.sql(
          s"""SELECT
             |DISTINCT(id)
             |FROM t
             |""".stripMargin)

        // df1.show(100)
        printPlan(df1)
      }

      /**
       * === Optimized Plan ===
       * 23:04:33.866 WARN org.apache.spark.sql.catalyst.rules.PlanChangeLogger:
       * === Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate ===
       * !Distinct                                                        Aggregate [id#39], [id#39]
       * +- Project [id#39]                                              +- Project [id#39]
       * +- RelationV2[id#39, name#40, value#41] paimon.default.t t      +- RelationV2[id#39, name#40, value#41] paimon.default.t t
       *
       *
       * Aggregate [id#39], [id#39]
       * +- RelationV2[id#39] t
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[id#39], functions=[], output=[id#39])
       * +- Exchange hashpartitioning(id#39, 5), ENSURE_REQUIREMENTS, [plan_id=50]
       * +- HashAggregate(keys=[id#39], functions=[], output=[id#39])
       * +- Project [id#39]
       * +- BatchScan t[id#39] PaimonScan: [t] RuntimeFilters: []
       */
    }
  }

  test(s"Aggregate: Sum group by") {
    withTable("t") {
      spark.sql(s"""
                   |CREATE TABLE t (id INT, name String, value INT)
                   |USING paimon
                   |tblproperties ('file.format' = 'parquet')
                   |""".stripMargin)

      spark.sql(
        s"""INSERT INTO t VALUES
           |(1, 'a', 1), (1, 'a', 2), (1, 'c', 3),
           |(2, 'd', 1), (2, 'e', 2),
           |(3, 'g', 1), (3, 'h', 2), (3, 'h', 3), (3, 'i', 4)
           |""".stripMargin)

      withSQLConf(
        "spark.sql.planChangeLog.rules" -> "") {
        val df1 = spark.sql(
          s"""
             |SELECT
             |id, SUM(value)
             |FROM t
             |GROUP BY id
             |""".stripMargin)

        printPlan(df1)
      }

      /**
       * === Optimized Plan ===
       * Aggregate [id#39], [id#39, sum(value#41) AS sum(value)#43L]
       * +- RelationV2[id#39, value#41] t
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[id#39], functions=[sum(value#41)], output=[id#39, sum(value)#43L])
       * +- Exchange hashpartitioning(id#39, 5), ENSURE_REQUIREMENTS, [plan_id=50]
       * +- HashAggregate(keys=[id#39], functions=[partial_sum(value#41)], output=[id#39, sum#51L])
       * +- Project [id#39, value#41]
       * +- BatchScan t[id#39, value#41] PaimonScan: [t] RuntimeFilters: []
       *
       * +---+----------+
       * | id|sum(value)|
       * +---+----------+
       * |  1|         6|
       * |  2|         3|
       * |  3|        10|
       * +---+----------+
       */
    }
  }

  test(s"Aggregate: Count distinct group by") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (key STRING, cat1 STRING, cat2 STRING, value INT)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""
           |INSERT INTO t1 VALUES
           |('a', 'ca1', 'cb2', 10),
           |('a', 'ca1', 'cb2', 5),
           |('b', 'ca1', 'cb2', 13)
           |""".stripMargin)

      val df = sql(
        s"""
           |SELECT
           |COUNT(DISTINCT cat1) AS cat1_cnt
           |FROM t1
           |GROUP BY key
           |""".stripMargin
      )

      printPlan(df)

      /**
       * === Optimized Plan ===
       * Aggregate [key#49], [count(distinct cat1#50) AS cat1_cnt#48L]
       * +- RelationV2[key#49, cat1#50] t1
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[key#49], functions=[count(distinct cat1#50)], output=[cat1_cnt#48L])
       * +- Exchange hashpartitioning(key#49, 5), ENSURE_REQUIREMENTS, [plan_id=60]
       * +- HashAggregate(keys=[key#49], functions=[partial_count(distinct cat1#50)], output=[key#49, count#61L])
       * +- HashAggregate(keys=[key#49, cat1#50], functions=[], output=[key#49, cat1#50])
       * +- Exchange hashpartitioning(key#49, cat1#50, 5), ENSURE_REQUIREMENTS, [plan_id=56]
       * +- HashAggregate(keys=[key#49, cat1#50], functions=[], output=[key#49, cat1#50])
       * +- Project [key#49, cat1#50]
       * +- BatchScan t1[key#49, cat1#50] PaimonScan: [t1] RuntimeFilters: []
       *
       * +--------+
       * |cat1_cnt|
       * +--------+
       * |       1|
       * |       1|
       * +--------+
       */
    }
  }

  test(s"Aggregate: Count filter group by") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (key STRING, cat1 STRING, cat2 STRING, value INT)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""
           |INSERT INTO t1 VALUES
           |('a', 'ca1', 'cb2', 10),
           |('a', 'ca1', 'cb2', 5),
           |('b', 'ca1', 'cb2', 13)
           |""".stripMargin)

      val df = sql(
        s"""
           |SELECT
           |key, COUNT(cat1) FILTER(WHERE value >= 10)
           |FROM t1
           |GROUP BY key
           |""".stripMargin
      )

      printPlan(df)

      /**
       * === Optimized Plan ===
       * Aggregate [key#48], [key#48, count(cat1#49) FILTER (WHERE (value#51 >= 10)) AS count(cat1) FILTER (WHERE (value >= 10))#53L]
       * +- RelationV2[key#48, cat1#49, value#51] t1
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[key#48], functions=[count(cat1#49)], output=[key#48, count(cat1) FILTER (WHERE (value >= 10))#53L])
       * +- Exchange hashpartitioning(key#48, 5), ENSURE_REQUIREMENTS, [plan_id=50]
       * +- HashAggregate(keys=[key#48], functions=[partial_count(cat1#49) FILTER (WHERE (value#51 >= 10))], output=[key#48, count#62L])
       * +- Project [key#48, cat1#49, value#51]
       * +- BatchScan t1[key#48, cat1#49, value#51] PaimonScan: [t1] RuntimeFilters: []
       *
       * +---+----------------------------------------+
       * |key|count(cat1) FILTER (WHERE (value >= 10))|
       * +---+----------------------------------------+
       * |  a|                                       1|
       * |  b|                                       1|
       * +---+----------------------------------------+
       */

    }
  }

  test(s"Aggregate: Count and sum group by") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (key STRING, cat1 STRING, cat2 STRING, value INT)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""
           |INSERT INTO t1 VALUES
           |('a', 'ca1', 'cb2', 10),
           |('a', 'ca1', 'cb2', 5),
           |('b', 'ca1', 'cb2', 13)
           |""".stripMargin)

      val df = sql(
        s"""
           |SELECT
           |key, COUNT(cat1), SUM(value)
           |FROM t1
           |GROUP BY key
           |""".stripMargin
      )

      printPlan(df)

      /**
       * === Optimized Plan ===
       * Aggregate [key#48], [key#48, count(cat1#49) AS count(cat1)#54L, sum(value#51) AS sum(value)#55L]
       * +- RelationV2[key#48, cat1#49, value#51] t1
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[key#48], functions=[count(cat1#49), sum(value#51)], output=[key#48, count(cat1)#54L, sum(value)#55L])
       * +- Exchange hashpartitioning(key#48, 5), ENSURE_REQUIREMENTS, [plan_id=50]
       * +- HashAggregate(keys=[key#48], functions=[partial_count(cat1#49), partial_sum(value#51)], output=[key#48, count#66L, sum#67L])
       * +- Project [key#48, cat1#49, value#51]
       * +- BatchScan t1[key#48, cat1#49, value#51] PaimonScan: [t1] RuntimeFilters: []
       *
       * +---+-----------+----------+
       * |key|count(cat1)|sum(value)|
       * +---+-----------+----------+
       * |  a|          2|        15|
       * |  b|          1|        13|
       * +---+-----------+----------+
       */
    }
  }
}
