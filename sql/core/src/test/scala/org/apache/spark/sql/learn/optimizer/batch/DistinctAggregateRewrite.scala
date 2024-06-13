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

package org.apache.spark.sql.learn.optimizer.batch

import org.apache.spark.sql.learn.BaseTest

class DistinctAggregateRewrite extends BaseTest {

  test(s"Distinct Aggregate Rewrite: One more distinct agg + regular agg") {
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
           |COUNT(DISTINCT cat1) AS cat1_cnt,
           |COUNT(DISTINCT cat2) AS cat2_cnt,
           |SUM(value) AS total
           |FROM t1
           |GROUP BY key
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.planChangeLog.rules" ->
          "org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates") {
        printPlan(df)
      }

      /**
       * === Applying Rule org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates ===
       * !Aggregate [key#51], [count(distinct cat1#52) AS cat1_cnt#48L, count(distinct cat2#53) AS cat2_cnt#49L, sum(value#54) AS total#50L]   Aggregate [key#51], [count(paimon.default.t1.cat1#68) FILTER (WHERE (gid#67 = 1)) AS cat1_cnt#48L, count(paimon.default.t1.cat2#69) FILTER (WHERE (gid#67 = 2)) AS cat2_cnt#49L, first(sum(paimon.default.t1.value)#71L, true) FILTER (WHERE (gid#67 = 0)) AS total#50L]
       * !+- RelationV2[key#51, cat1#52, cat2#53, value#54] t1                                                                                 +- Aggregate [key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], [key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, sum(paimon.default.t1.value#70) AS sum(paimon.default.t1.value)#71L]
       * !                                                                                                                                        +- Expand [[key#51, null, null, 0, value#54], [key#51, cat1#52, null, 1, null], [key#51, null, cat2#53, 2, null]], [key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, paimon.default.t1.value#70]
       * !                                                                                                                                           +- RelationV2[key#51, cat1#52, cat2#53, value#54] t1
       */

      /**
       * === Optimized Plan ===
       * Aggregate [key#51], [count(paimon.default.t1.cat1#68) FILTER (WHERE (gid#67 = 1)) AS cat1_cnt#48L, count(paimon.default.t1.cat2#69) FILTER (WHERE (gid#67 = 2)) AS cat2_cnt#49L, first(sum(paimon.default.t1.value)#71L, true) FILTER (WHERE (gid#67 = 0)) AS total#50L]
       * +- Aggregate [key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], [key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, sum(paimon.default.t1.value#70) AS sum(paimon.default.t1.value)#71L]
       * +- Expand [[key#51, null, null, 0, value#54], [key#51, cat1#52, null, 1, null], [key#51, null, cat2#53, 2, null]], [key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, paimon.default.t1.value#70]
       * +- RelationV2[key#51, cat1#52, cat2#53, value#54] t1
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[key#51], functions=[count(paimon.default.t1.cat1#68), count(paimon.default.t1.cat2#69), first(sum(paimon.default.t1.value)#71L, true)], output=[cat1_cnt#48L, cat2_cnt#49L, total#50L])
       * +- Exchange hashpartitioning(key#51, 5), ENSURE_REQUIREMENTS, [plan_id=65]
       * +- HashAggregate(keys=[key#51], functions=[partial_count(paimon.default.t1.cat1#68) FILTER (WHERE (gid#67 = 1)), partial_count(paimon.default.t1.cat2#69) FILTER (WHERE (gid#67 = 2)), partial_first(sum(paimon.default.t1.value)#71L, true) FILTER (WHERE (gid#67 = 0))], output=[key#51, count#77L, count#78L, first#79L, valueSet#80])
       * +- HashAggregate(keys=[key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], functions=[sum(paimon.default.t1.value#70)], output=[key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, sum(paimon.default.t1.value)#71L])
       * +- Exchange hashpartitioning(key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, 5), ENSURE_REQUIREMENTS, [plan_id=61]
       * +- HashAggregate(keys=[key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], functions=[partial_sum(paimon.default.t1.value#70)], output=[key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, sum#82L])
       * +- Expand [[key#51, null, null, 0, value#54], [key#51, cat1#52, null, 1, null], [key#51, null, cat2#53, 2, null]], [key#51, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, paimon.default.t1.value#70]
       * +- Project [key#51, cat1#52, cat2#53, value#54]
       * +- BatchScan t1[key#51, cat1#52, cat2#53, value#54] PaimonScan: [t1] RuntimeFilters: []
       *
       * +--------+--------+-----+
       * |cat1_cnt|cat2_cnt|total|
       * +--------+--------+-----+
       * |       1|       1|   15|
       * |       1|       1|   13|
       * +--------+--------+-----+
       */
    }
  }

  test(s"Distinct Aggregate Rewrite: Distinct filter agg") {
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
           |key, COUNT(DISTINCT cat1) FILTER(WHERE value >= 10)
           |FROM t1
           |GROUP BY key
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.planChangeLog.rules" ->
          "org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates") {
        printPlan(df)
      }

      /**
       * === Applying Rule org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates ===
       * !Aggregate [key#48], [key#48, count(distinct cat1#49) FILTER (WHERE (value#51 >= 10)) AS count(DISTINCT cat1) FILTER (WHERE (value >= 10))#53L]   Aggregate [key#48], [key#48, count(paimon.default.t1.cat1#62) FILTER (WHERE ((gid#61 = 1) AND (paimon.default.t1.value >= 10)#65)) AS count(DISTINCT cat1) FILTER (WHERE (value >= 10))#53L]
       * !+- RelationV2[key#48, cat1#49, value#51] t1                                                                                                      +- Aggregate [key#48, paimon.default.t1.cat1#62, gid#61], [key#48, paimon.default.t1.cat1#62, gid#61, max((paimon.default.t1.value >= 10)#63) AS (paimon.default.t1.value >= 10)#65]
       * !                                                                                                                                                    +- Expand [[key#48, cat1#49, 1, (value#51 >= 10)]], [key#48, paimon.default.t1.cat1#62, gid#61, (paimon.default.t1.value >= 10)#63]
       * !                                                                                                                                                       +- RelationV2[key#48, cat1#49, value#51] t1
       */

      /**
       * === Optimized Plan ===
       * Aggregate [key#48], [key#48, count(paimon.default.t1.cat1#62) FILTER (WHERE ((gid#61 = 1) AND (paimon.default.t1.value >= 10)#65)) AS count(DISTINCT cat1) FILTER (WHERE (value >= 10))#53L]
       * +- Aggregate [key#48, paimon.default.t1.cat1#62, gid#61], [key#48, paimon.default.t1.cat1#62, gid#61, max((paimon.default.t1.value >= 10)#63) AS (paimon.default.t1.value >= 10)#65]
       * +- Expand [[key#48, cat1#49, 1, (value#51 >= 10)]], [key#48, paimon.default.t1.cat1#62, gid#61, (paimon.default.t1.value >= 10)#63]
       * +- RelationV2[key#48, cat1#49, value#51] t1
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[key#48], functions=[count(paimon.default.t1.cat1#62)], output=[key#48, count(DISTINCT cat1) FILTER (WHERE (value >= 10))#53L])
       * +- Exchange hashpartitioning(key#48, 5), ENSURE_REQUIREMENTS, [plan_id=65]
       * +- HashAggregate(keys=[key#48], functions=[partial_count(paimon.default.t1.cat1#62) FILTER (WHERE ((gid#61 = 1) AND (paimon.default.t1.value >= 10)#65))], output=[key#48, count#67L])
       * +- HashAggregate(keys=[key#48, paimon.default.t1.cat1#62, gid#61], functions=[max((paimon.default.t1.value >= 10)#63)], output=[key#48, paimon.default.t1.cat1#62, gid#61, (paimon.default.t1.value >= 10)#65])
       * +- Exchange hashpartitioning(key#48, paimon.default.t1.cat1#62, gid#61, 5), ENSURE_REQUIREMENTS, [plan_id=61]
       * +- HashAggregate(keys=[key#48, paimon.default.t1.cat1#62, gid#61], functions=[partial_max((paimon.default.t1.value >= 10)#63)], output=[key#48, paimon.default.t1.cat1#62, gid#61, max#69])
       * +- Expand [[key#48, cat1#49, 1, (value#51 >= 10)]], [key#48, paimon.default.t1.cat1#62, gid#61, (paimon.default.t1.value >= 10)#63]
       * +- Project [key#48, cat1#49, value#51]
       * +- BatchScan t1[key#48, cat1#49, value#51] PaimonScan: [t1] RuntimeFilters: []
       *
       * +---+-------------------------------------------------+
       * |key|count(DISTINCT cat1) FILTER (WHERE (value >= 10))|
       * +---+-------------------------------------------------+
       * |  a|                                                1|
       * |  b|                                                1|
       * +---+-------------------------------------------------+
       */
    }
  }

  test(s"Distinct Aggregate Rewrite: One more distinct filter agg + regular filter agg") {
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
           |COUNT(DISTINCT cat1) FILTER(WHERE value >= 1),
           |COUNT(DISTINCT cat2) FILTER(WHERE value >= 2),
           |SUM(value) FILTER(WHERE value >= 3)
           |FROM t1
           |GROUP BY key
           |""".stripMargin
      )

      withSQLConf(
        "spark.sql.planChangeLog.rules" ->
          "org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates") {
        printPlan(df)
      }

      /**
       * === Optimized Plan ===
       * 16:20:50.667 ERROR org.apache.spark.sql.catalyst.rules.PlanChangeLogger:
       * === Applying Rule org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates ===
       * !Aggregate [key#48], [count(distinct cat1#49) FILTER (WHERE (value#51 >= 1)) AS count(DISTINCT cat1) FILTER (WHERE (value >= 1))#55L, count(distinct cat2#50) FILTER (WHERE (value#51 >= 2)) AS count(DISTINCT cat2) FILTER (WHERE (value >= 2))#56L, sum(value#51) FILTER (WHERE (value#51 >= 3)) AS sum(value) FILTER (WHERE (value >= 3))#57L]   Aggregate [key#48], [count(paimon.default.t1.cat1#68) FILTER (WHERE ((gid#67 = 1) AND (paimon.default.t1.value >= 1)#72)) AS count(DISTINCT cat1) FILTER (WHERE (value >= 1))#55L, count(paimon.default.t1.cat2#69) FILTER (WHERE ((gid#67 = 2) AND (paimon.default.t1.value >= 2)#75)) AS count(DISTINCT cat2) FILTER (WHERE (value >= 2))#56L, first(sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#77L, true) FILTER (WHERE (gid#67 = 0)) AS sum(value) FILTER (WHERE (value >= 3))#57L]
       * !+- RelationV2[key#48, cat1#49, cat2#50, value#51] t1                                                                                                                                                                                                                                                                                               +- Aggregate [key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], [key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, max((paimon.default.t1.value >= 1)#70) AS (paimon.default.t1.value >= 1)#72, max((paimon.default.t1.value >= 2)#73) AS (paimon.default.t1.value >= 2)#75, sum(paimon.default.t1.value#76) FILTER (WHERE (paimon.default.t1.value#76 >= 3)) AS sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#77L]
       * !                                                                                                                                                                                                                                                                                                                                                      +- Expand [[key#48, null, null, 0, null, null, value#51], [key#48, cat1#49, null, 1, (value#51 >= 1), null, null], [key#48, null, cat2#50, 2, null, (value#51 >= 2), null]], [key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, (paimon.default.t1.value >= 1)#70, (paimon.default.t1.value >= 2)#73, paimon.default.t1.value#76]
       * !                                                                                                                                                                                                                                                                                                                                                         +- RelationV2[key#48, cat1#49, cat2#50, value#51] t1
       *
       *
       * Aggregate [key#48], [count(paimon.default.t1.cat1#68) FILTER (WHERE ((gid#67 = 1) AND (paimon.default.t1.value >= 1)#72)) AS count(DISTINCT cat1) FILTER (WHERE (value >= 1))#55L, count(paimon.default.t1.cat2#69) FILTER (WHERE ((gid#67 = 2) AND (paimon.default.t1.value >= 2)#75)) AS count(DISTINCT cat2) FILTER (WHERE (value >= 2))#56L, first(sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#77L, true) FILTER (WHERE (gid#67 = 0)) AS sum(value) FILTER (WHERE (value >= 3))#57L]
       * +- Aggregate [key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], [key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, max((paimon.default.t1.value >= 1)#70) AS (paimon.default.t1.value >= 1)#72, max((paimon.default.t1.value >= 2)#73) AS (paimon.default.t1.value >= 2)#75, sum(paimon.default.t1.value#76) FILTER (WHERE (paimon.default.t1.value#76 >= 3)) AS sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#77L]
       * +- Expand [[key#48, null, null, 0, null, null, value#51], [key#48, cat1#49, null, 1, (value#51 >= 1), null, null], [key#48, null, cat2#50, 2, null, (value#51 >= 2), null]], [key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, (paimon.default.t1.value >= 1)#70, (paimon.default.t1.value >= 2)#73, paimon.default.t1.value#76]
       * +- RelationV2[key#48, cat1#49, cat2#50, value#51] t1
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[key#48], functions=[count(paimon.default.t1.cat1#68), count(paimon.default.t1.cat2#69), first(sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#77L, true)], output=[count(DISTINCT cat1) FILTER (WHERE (value >= 1))#55L, count(DISTINCT cat2) FILTER (WHERE (value >= 2))#56L, sum(value) FILTER (WHERE (value >= 3))#57L])
       * +- Exchange hashpartitioning(key#48, 5), ENSURE_REQUIREMENTS, [plan_id=65]
       * +- HashAggregate(keys=[key#48], functions=[partial_count(paimon.default.t1.cat1#68) FILTER (WHERE ((gid#67 = 1) AND (paimon.default.t1.value >= 1)#72)), partial_count(paimon.default.t1.cat2#69) FILTER (WHERE ((gid#67 = 2) AND (paimon.default.t1.value >= 2)#75)), partial_first(sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#77L, true) FILTER (WHERE (gid#67 = 0))], output=[key#48, count#83L, count#84L, first#85L, valueSet#86])
       * +- HashAggregate(keys=[key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], functions=[max((paimon.default.t1.value >= 1)#70), max((paimon.default.t1.value >= 2)#73), sum(paimon.default.t1.value#76)], output=[key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, (paimon.default.t1.value >= 1)#72, (paimon.default.t1.value >= 2)#75, sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#77L])
       * +- Exchange hashpartitioning(key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, 5), ENSURE_REQUIREMENTS, [plan_id=61]
       * +- HashAggregate(keys=[key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67], functions=[partial_max((paimon.default.t1.value >= 1)#70), partial_max((paimon.default.t1.value >= 2)#73), partial_sum(paimon.default.t1.value#76) FILTER (WHERE (paimon.default.t1.value#76 >= 3))], output=[key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, max#90, max#91, sum#92L])
       * +- Expand [[key#48, null, null, 0, null, null, value#51], [key#48, cat1#49, null, 1, (value#51 >= 1), null, null], [key#48, null, cat2#50, 2, null, (value#51 >= 2), null]], [key#48, paimon.default.t1.cat1#68, paimon.default.t1.cat2#69, gid#67, (paimon.default.t1.value >= 1)#70, (paimon.default.t1.value >= 2)#73, paimon.default.t1.value#76]
       * +- Project [key#48, cat1#49, cat2#50, value#51]
       * +- BatchScan t1[key#48, cat1#49, cat2#50, value#51] PaimonScan: [t1] RuntimeFilters: []
       *
       * 16:20:50.870 ERROR org.apache.spark.sql.catalyst.rules.PlanChangeLogger:
       * === Applying Rule org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates ===
       * GlobalLimit 21                                                                                                                                                                                                                                                                                                                                                                                        GlobalLimit 21
       * +- LocalLimit 21                                                                                                                                                                                                                                                                                                                                                                                      +- LocalLimit 21
       * !   +- Aggregate [key#48], [cast(count(distinct cat1#49) FILTER (WHERE (value#51 >= 1)) as string) AS count(DISTINCT cat1) FILTER (WHERE (value >= 1))#96, cast(count(distinct cat2#50) FILTER (WHERE (value#51 >= 2)) as string) AS count(DISTINCT cat2) FILTER (WHERE (value >= 2))#97, cast(sum(value#51) FILTER (WHERE (value#51 >= 3)) as string) AS sum(value) FILTER (WHERE (value >= 3))#98]      +- Aggregate [key#48], [cast(count(paimon.default.t1.cat1#107) FILTER (WHERE ((gid#106 = 1) AND (paimon.default.t1.value >= 1)#111)) as string) AS count(DISTINCT cat1) FILTER (WHERE (value >= 1))#96, cast(count(paimon.default.t1.cat2#108) FILTER (WHERE ((gid#106 = 2) AND (paimon.default.t1.value >= 2)#114)) as string) AS count(DISTINCT cat2) FILTER (WHERE (value >= 2))#97, cast(first(sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#116L, true) FILTER (WHERE (gid#106 = 0)) as string) AS sum(value) FILTER (WHERE (value >= 3))#98]
       * !      +- RelationV2[key#48, cat1#49, cat2#50, value#51] t1                                                                                                                                                                                                                                                                                                                                                  +- Aggregate [key#48, paimon.default.t1.cat1#107, paimon.default.t1.cat2#108, gid#106], [key#48, paimon.default.t1.cat1#107, paimon.default.t1.cat2#108, gid#106, max((paimon.default.t1.value >= 1)#109) AS (paimon.default.t1.value >= 1)#111, max((paimon.default.t1.value >= 2)#112) AS (paimon.default.t1.value >= 2)#114, sum(paimon.default.t1.value#115) FILTER (WHERE (paimon.default.t1.value#115 >= 3)) AS sum(paimon.default.t1.value) FILTER (WHERE (paimon.default.t1.value >= 3))#116L]
       * !                                                                                                                                                                                                                                                                                                                                                                                                               +- Expand [[key#48, null, null, 0, null, null, value#51], [key#48, cat1#49, null, 1, (value#51 >= 1), null, null], [key#48, null, cat2#50, 2, null, (value#51 >= 2), null]], [key#48, paimon.default.t1.cat1#107, paimon.default.t1.cat2#108, gid#106, (paimon.default.t1.value >= 1)#109, (paimon.default.t1.value >= 2)#112, paimon.default.t1.value#115]
       * !                                                                                                                                                                                                                                                                                                                                                                                                                  +- RelationV2[key#48, cat1#49, cat2#50, value#51] t1
       *
       *
       * +------------------------------------------------+------------------------------------------------+--------------------------------------+
       * |count(DISTINCT cat1) FILTER (WHERE (value >= 1))|count(DISTINCT cat2) FILTER (WHERE (value >= 2))|sum(value) FILTER (WHERE (value >= 3))|
       * +------------------------------------------------+------------------------------------------------+--------------------------------------+
       * |                                               1|                                               1|                                    15|
       * |                                               1|                                               1|                                    13|
       * +------------------------------------------------+------------------------------------------------+--------------------------------------+
       */
    }
  }
}
