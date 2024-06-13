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

class Join extends BaseTest {

  test(s"Join: Simple example") {
    withTable("t1", "t2") {
      sql(
        s"""
           |CREATE TABLE t1 (id INT, name String, id_ext INT)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""INSERT INTO t1 VALUES
           |(1, 'a', 1),
           |(2, 'b', 2),
           |(3, 'a', 2),
           |(4, 'c', 1)
           |""".stripMargin)

      sql(
        s"""
           |CREATE TABLE t2 (id INT, size INT)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""INSERT INTO t2 VALUES
           |(1, 11),
           |(2, 22),
           |(3, 33)
           |""".stripMargin)

      val df1 = sql(
        s"""SELECT
           |t1.id, t1.name, t1.id_ext, t2.size
           |FROM t1, t2
           |WHERE t1.id_ext = t2.id
           |""".stripMargin)

      printPlan(df1)

      /**
       * === Optimized Plan ===
       * Project [id#69, name#70, id_ext#71, size#73]
       * +- Join Inner, (id_ext#71 = id#72)
       * :- Filter isnotnull(id_ext#71)
       * :  +- RelationV2[id#69, name#70, id_ext#71] default.t1
       * +- Filter isnotnull(id#72)
       * +- RelationV2[id#72, size#73] default.t2
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- Project [id#69, name#70, id_ext#71, size#73]
       * +- BroadcastHashJoin [id_ext#71], [id#72], Inner, BuildRight, false
       * :- Project [id#69, name#70, id_ext#71]
       * :  +- Filter isnotnull(id_ext#71)
       * :     +- BatchScan default.t1[id#69, name#70, id_ext#71] PaimonScan: [t1], PushedFilters: [IsNotNull(id_ext)] RuntimeFilters: []
       * +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=92]
       * +- Project [id#72, size#73]
       * +- Filter isnotnull(id#72)
       * +- BatchScan default.t2[id#72, size#73] PaimonScan: [t2], PushedFilters: [IsNotNull(id)] RuntimeFilters: []
       *
       * +---+----+------+----+
       * | id|name|id_ext|size|
       * +---+----+------+----+
       * |  1|   a|     1|  11|
       * |  2|   b|     2|  22|
       * |  3|   a|     2|  22|
       * |  4|   c|     1|  11|
       * +---+----+------+----+
       */
    }
  }

  test(s"Join: join + agg") {
    withTable("t1", "t2") {
      sql(
        s"""
           |CREATE TABLE t1 (a1 INT, b1 STRING, d1 INT)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""INSERT INTO t1 VALUES
           |(1, 'x', 11),
           |(2, 'y', 33),
           |(3, 'y', 22)
           |""".stripMargin)

      sql(s"ANALYZE TABLE t1 COMPUTE STATISTICS FOR COLUMNS a1, b1, d1")

      sql(
        s"""
           |CREATE TABLE t2 (id INT, c2 STRING, e2 INT, a2 INT)
           |USING paimon
           |tblproperties ('file.format' = 'parquet')
           |""".stripMargin)

      sql(
        s"""INSERT INTO t2 VALUES
           |(1, "a", 11, 1),
           |(2, "c", 33, 3),
           |(3, "b", 11, 2),
           |(4, "a", 22, 1)
           |""".stripMargin)

      sql(s"ANALYZE TABLE t2 COMPUTE STATISTICS FOR COLUMNS id, c2, e2, a2")

      val df1 = sql(
        s"""SELECT
           |t1.b1, t2.c2, sum(t1.d1), min(t2.e2)
           |FROM t1, t2
           |WHERE t1.a1 = t2.a2
           |GROUP BY t1.b1, t2.c2
           |""".stripMargin)

      // printPlan(df1)

      /**
       * === Optimized Plan ===
       * Aggregate [b1#88, c2#91], [b1#88, c2#91, sum(d1#89) AS sum(d1)#96L, min(e2#92) AS min(e2)#97]
       * +- Project [b1#88, d1#89, c2#91, e2#92]
       * +- Join Inner, (a1#87 = a2#93)
       * :- Filter isnotnull(a1#87)
       * :  +- RelationV2[a1#87, b1#88, d1#89] default.t1
       * +- Filter isnotnull(a2#93)
       * +- RelationV2[c2#91, e2#92, a2#93] default.t2
       *
       * === Executed Plan ===
       * AdaptiveSparkPlan isFinalPlan=false
       * +- HashAggregate(keys=[b1#88, c2#91], functions=[sum(d1#89), min(e2#92)], output=[b1#88, c2#91, sum(d1)#96L, min(e2)#97])
       * +- Exchange hashpartitioning(b1#88, c2#91, 5), ENSURE_REQUIREMENTS, [plan_id=104]
       * +- HashAggregate(keys=[b1#88, c2#91], functions=[partial_sum(d1#89), partial_min(e2#92)], output=[b1#88, c2#91, sum#114L, min#115])
       * +- Project [b1#88, d1#89, c2#91, e2#92]
       * +- BroadcastHashJoin [a1#87], [a2#93], Inner, BuildLeft, false
       * :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=99]
       * :  +- Project [a1#87, b1#88, d1#89]
       * :     +- Filter isnotnull(a1#87)
       * :        +- BatchScan default.t1[a1#87, b1#88, d1#89] PaimonScan: [t1], PushedFilters: [IsNotNull(a1)] RuntimeFilters: []
       * +- Project [c2#91, e2#92, a2#93]
       * +- Filter isnotnull(a2#93)
       * +- BatchScan default.t2[c2#91, e2#92, a2#93] PaimonScan: [t2], PushedFilters: [IsNotNull(a2)] RuntimeFilters: []
       *
       * +---+---+-------+-------+
       * | b1| c2|sum(d1)|min(e2)|
       * +---+---+-------+-------+
       * |  y|  c|     22|     33|
       * |  x|  a|     22|     11|
       * |  y|  b|     33|     11|
       * +---+---+-------+-------+
       */

      withSQLConf(
        "spark.sql.optimizer.pushdownAggregateBelowJoin" -> "true",
        "spark.sql.optimizer.pushdownAggregateBelowJoin.ratio" -> "1",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1"
      ) {
        printPlan(df1)
      }
    }
  }

}
