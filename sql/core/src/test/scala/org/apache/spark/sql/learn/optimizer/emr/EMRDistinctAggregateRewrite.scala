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

class EMRDistinctAggregateRewrite extends BaseTest{

  test(s"EMR Optimizer: Single Type Distinct Aggregate Rewrite") {
    withTable("t1") {
      sql(
        s"""
           |CREATE TABLE t1 (key INT, s1 STRING, s2 STRING)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""
           |INSERT INTO t1 VALUES
           |(1, 'a', 'a'), (1, 'a', 'b'), (1, 'b', 'c'), (1, 'a', 'd'),
           |(2, 'a', 'a'), (2, 'a', 'a'), (2, 'b', 'a'), (2, 'a', 'a'),
           |(3, 'a', 'a'), (3, 'b', 'a'), (3, 'c', 'a'), (3, 'd', 'd')
           |""".stripMargin
      )

      val df = sql(
        s"""
           |SELECT
           |key, COUNT(DISTINCT s1), COUNT(DISTINCT s2)
           |FROM t1
           |GROUP BY key
           |""".stripMargin
      )

      // printPlan(df)

      /**
       * === Optimized Plan ===
       * Aggregate [key#39], [key#39, count(paimon.default.t1.s1#55) FILTER (WHERE (gid#54 = 1)) AS count(DISTINCT s1)#44L, count(paimon.default.t1.s2#56) FILTER (WHERE (gid#54 = 2)) AS count(DISTINCT s2)#45L]
       * +- Aggregate [key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54], [key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54]
       * +- Expand [[key#39, s1#40, null, 1], [key#39, null, s2#41, 2]], [key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54]
       * +- RelationV2[key#39, s1#40, s2#41] t1
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[key#39], functions=[count(paimon.default.t1.s1#55), count(paimon.default.t1.s2#56)], output=[key#39, count(DISTINCT s1)#44L, count(DISTINCT s2)#45L])
       * +- Exchange hashpartitioning(key#39, 5), ENSURE_REQUIREMENTS, [plan_id=65]
       * +- HashAggregate(keys=[key#39], functions=[partial_count(paimon.default.t1.s1#55) FILTER (WHERE (gid#54 = 1)), partial_count(paimon.default.t1.s2#56) FILTER (WHERE (gid#54 = 2))], output=[key#39, count#59L, count#60L])
       * +- HashAggregate(keys=[key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54], functions=[], output=[key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54])
       * +- Exchange hashpartitioning(key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54, 5), ENSURE_REQUIREMENTS, [plan_id=61]
       * +- HashAggregate(keys=[key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54], functions=[], output=[key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54])
       * +- Expand [[key#39, s1#40, null, 1], [key#39, null, s2#41, 2]], [key#39, paimon.default.t1.s1#55, paimon.default.t1.s2#56, gid#54]
       * +- Project [key#39, s1#40, s2#41]
       * +- BatchScan t1[key#39, s1#40, s2#41] PaimonScan: [t1] RuntimeFilters: []
       *
       * +---+------------------+------------------+
       * |key|count(DISTINCT s1)|count(DISTINCT s2)|
       * +---+------------------+------------------+
       * |  3|                 4|                 2|
       * |  1|                 2|                 4|
       * |  2|                 2|                 1|
       * +---+------------------+------------------+
       */

      withSQLConf("spark.sql.optimizer.rewriteSingleTypeDistinctAggregates" -> "true") {
        printPlan(df)

        /**
         * === Optimized Plan ===
         * Aggregate [key#39], [key#39, count(if ((gid#54 = 1)) merged_child#55 else null) AS count(DISTINCT s1)#44L, count(if ((gid#54 = 2)) merged_child#55 else null) AS count(DISTINCT s2)#45L]
         * +- Aggregate [key#39, merged_child#55, gid#54], [key#39, merged_child#55, gid#54]
         * +- Expand [[key#39, s1#40, 1], [key#39, s2#41, 2]], [key#39, merged_child#55, gid#54]
         * +- RelationV2[key#39, s1#40, s2#41] t1
         *
         * === Executed Plan ===
         * AdaptiveSparkPlan isFinalPlan=false
         * +- HashAggregate(keys=[key#39], functions=[count(if ((gid#54 = 1)) merged_child#55 else null), count(if ((gid#54 = 2)) merged_child#55 else null)], output=[key#39, count(DISTINCT s1)#44L, count(DISTINCT s2)#45L])
         * +- Exchange hashpartitioning(key#39, 5), ENSURE_REQUIREMENTS, [plan_id=65]
         * +- HashAggregate(keys=[key#39], functions=[partial_count(if ((gid#54 = 1)) merged_child#55 else null), partial_count(if ((gid#54 = 2)) merged_child#55 else null)], output=[key#39, count#58L, count#59L])
         * +- HashAggregate(keys=[key#39, merged_child#55, gid#54], functions=[], output=[key#39, merged_child#55, gid#54])
         * +- Exchange hashpartitioning(key#39, merged_child#55, gid#54, 5), ENSURE_REQUIREMENTS, [plan_id=61]
         * +- HashAggregate(keys=[key#39, merged_child#55, gid#54], functions=[], output=[key#39, merged_child#55, gid#54])
         * +- Expand [[key#39, s1#40, 1], [key#39, s2#41, 2]], [key#39, merged_child#55, gid#54]
         * +- Project [key#39, s1#40, s2#41]
         * +- BatchScan t1[key#39, s1#40, s2#41] PaimonScan: [t1] RuntimeFilters: []
         *
         * +---+------------------+------------------+
         * |key|count(DISTINCT s1)|count(DISTINCT s2)|
         * +---+------------------+------------------+
         * |  3|                 4|                 2|
         * |  1|                 2|                 4|
         * |  2|                 2|                 1|
         * +---+------------------+------------------+
         */
      }
    }
  }
}
