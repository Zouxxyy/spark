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

class WindowTopKFilter extends BaseTest {

  test(s"Optimizer: Window TopK Filter") {
    withTable("dw2") {
      spark.sql(s"""
                   |CREATE TABLE dw2 (i_category STRING, sumsales INT)
                   |USING paimon
                   |tblproperties ('file.format' = 'parquet')
                   |""".stripMargin)

      spark.sql(
        s"""INSERT INTO dw2 VALUES
           |('c1', 1), ('c1', 3), ('c1', 9), ('c1', 12), ('c1', 16), ('c1', 18),
           |('c2', 2), ('c2', 4), ('c2', 5), ('c2', 5), ('c2', 20),
           |('c3', 1), ('c3', 7), ('c3', 9), ('c3', 11),
           |('c4', 3), ('c4', 8), ('c4', 12), ('c4', 17), ('c4', 21)
           |""".stripMargin)

      val df = spark.sql(
        s"""
           |select *
           |from (select i_category,
           |             sumsales,
           |             rank() over (partition by i_category order by sumsales desc) rk
           |      from dw2)
           |where rk <= 3
           |limit 100
           |""".stripMargin)
      df.show(false)
      /**
       * +----------+--------+---+
       * |i_category|sumsales|rk |
       * +----------+--------+---+
       * |c1        |18      |1  |
       * |c1        |16      |2  |
       * |c1        |12      |3  |
       * |c2        |20      |1  |
       * |c2        |5       |2  |
       * |c2        |5       |2  |
       * |c3        |11      |1  |
       * |c3        |9       |2  |
       * |c3        |7       |3  |
       * |c4        |21      |1  |
       * |c4        |17      |2  |
       * |c4        |12      |3  |
       * +----------+--------+---+
       */

      // scalastyle:off println
      // println(df.queryExecution.optimizedPlan)
      // scalastyle:on println

      /**
       * GlobalLimit 100
       * +- LocalLimit 100
       * +- Filter (rk#30 <= 3)
       * +- Window [rank(sumsales#32) windowspecdefinition(i_category#31, sumsales#32 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rk#30], [i_category#31], [sumsales#32 DESC NULLS LAST]
       * +- RelationV2[i_category#31, sumsales#32] dw2
       */

      // scalastyle:off println
      // println(df.queryExecution.executedPlan)
      // scalastyle:on println

      /**
       * AdaptiveSparkPlan isFinalPlan=false
       * +- CollectLimit 100
       * +- Filter (rk#30 <= 3)
       * +- Window [rank(sumsales#32) windowspecdefinition(i_category#31, sumsales#32 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rk#30], [i_category#31], [sumsales#32 DESC NULLS LAST]
       * +- Sort [i_category#31 ASC NULLS FIRST, sumsales#32 DESC NULLS LAST], false, 0
       * +- Exchange hashpartitioning(i_category#31, 5), ENSURE_REQUIREMENTS, [plan_id=127]
       * +- Project [i_category#31, sumsales#32]
       * +- BatchScan dw2[i_category#31, sumsales#32] PaimonScan: [dw2] RuntimeFilters: []
       */

      withSQLConf(
        "spark.sql.windowTopKFilter.pushdown" -> "true",
        "spark.sql.windowTopKFilter.sortFallbackThreshold" -> "4",
        "spark.sql.windowTopKFilter.maxCardinality" -> "10") {
        // scalastyle:off println
        println(df.queryExecution.executedPlan)
        // scalastyle:on println
      }

      /**
       * AdaptiveSparkPlan isFinalPlan=false
       * +- CollectLimit 100
       * +- Filter (rk#30 <= 3)
       * +- Window [rank(sumsales#32) windowspecdefinition(i_category#31, sumsales#32 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rk#30], [i_category#31], [sumsales#32 DESC NULLS LAST]
       * +- Sort [i_category#31 ASC NULLS FIRST, sumsales#32 DESC NULLS LAST], false, 0
       * +- Exchange hashpartitioning(i_category#31, 5), ENSURE_REQUIREMENTS, [plan_id=131]
       * +- WindowTopKFilter 3, 1, [i_category#31], [sumsales#32 DESC NULLS LAST]
       * +- Project [i_category#31, sumsales#32]
       * +- BatchScan dw2[i_category#31, sumsales#32] PaimonScan: [dw2] RuntimeFilters: []
       */
    }
  }
}
