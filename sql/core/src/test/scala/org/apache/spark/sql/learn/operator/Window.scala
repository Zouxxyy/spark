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

class Window extends BaseTest {

  test(s"Operator: Window") {
    withTable("dw2") {
      spark.sql(s"""
                   |CREATE TABLE dw2 (i_category STRING, sales INT)
                   |USING paimon
                   |tblproperties ('file.format' = 'parquet')
                   |""".stripMargin)

      spark.sql(
        s"""INSERT INTO dw2 VALUES
           |('c1', 3), ('c1', 1), ('c1', 9),
           |('c2', 2), ('c2', 20), ('c2', 5), ('c2', 5), ('c2', 4),
           |('c3', 1), ('c3', 9), ('c3', 7), ('c3', 11)
           |""".stripMargin)

      val df1 = spark.sql(
        s"""
           |select
           |  i_category,
           |  sales,
           |  rank() over (partition by i_category order by sales) as rk
           |from dw2
           |""".stripMargin)
      df1.show(false)
      // scalastyle:off println
      println(df1.queryExecution.executedPlan)
      // scalastyle:on println

      /**
       * +----------+-----+---+
       * |i_category|sales|rk |
       * +----------+-----+---+
       * |c1        |1    |1  |
       * |c1        |3    |2  |
       * |c1        |9    |3  |
       * |c2        |2    |1  |
       * |c2        |4    |2  |
       * |c2        |5    |3  |
       * |c2        |5    |3  |
       * |c2        |20   |5  |
       * |c3        |1    |1  |
       * |c3        |7    |2  |
       * |c3        |9    |3  |
       * |c3        |11   |4  |
       * +----------+-----+---+
       *
       * AdaptiveSparkPlan isFinalPlan=false
       * +- Window [rank(sales#32) windowspecdefinition(i_category#31, sales#32 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rk#30], [i_category#31], [sales#32 ASC NULLS FIRST]
       * +- Sort [i_category#31 ASC NULLS FIRST, sales#32 ASC NULLS FIRST], false, 0
       * +- Exchange hashpartitioning(i_category#31, 5), ENSURE_REQUIREMENTS, [plan_id=108]
       * +- Project [i_category#31, sales#32]
       * +- BatchScan dw2[i_category#31, sales#32] PaimonScan: [dw2] RuntimeFilters: []
       */

      val df2 = spark.sql(
        s"""
           |select
           |  i_category,
           |  sales,
           |  min(sales) over (partition by i_category order by sales) as min
           |from dw2
           |""".stripMargin)
      df2.show(false)
      // scalastyle:off println
      println(df2.queryExecution.executedPlan)
      // scalastyle:on println

      /**
       * +----------+-----+---+
       * |i_category|sales|min|
       * +----------+-----+---+
       * |c1        |1    |1  |
       * |c1        |3    |1  |
       * |c1        |9    |1  |
       * |c2        |2    |2  |
       * |c2        |4    |2  |
       * |c2        |5    |2  |
       * |c2        |5    |2  |
       * |c2        |20   |2  |
       * |c3        |1    |1  |
       * |c3        |7    |1  |
       * |c3        |9    |1  |
       * |c3        |11   |1  |
       * +----------+-----+---+
       *
       * AdaptiveSparkPlan isFinalPlan=false
       * +- Window [min(sales#65) windowspecdefinition(i_category#64, sales#65 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS min#63], [i_category#64], [sales#65 ASC NULLS FIRST]
       * +- Sort [i_category#64 ASC NULLS FIRST, sales#65 ASC NULLS FIRST], false, 0
       * +- Exchange hashpartitioning(i_category#64, 5), ENSURE_REQUIREMENTS, [plan_id=182]
       * +- Project [i_category#64, sales#65]
       * +- BatchScan dw2[i_category#64, sales#65] PaimonScan: [dw2] RuntimeFilters: []
       */

      val df3 = spark.sql(
        s"""
           |select
           |  i_category,
           |  sales,
           |  min(sales) over (partition by i_category) as min
           |from dw2
           |""".stripMargin)
      df3.show(false)
      // scalastyle:off println
      println(df3.queryExecution.executedPlan)
      // scalastyle:on println

      /**
       * +----------+-----+---+
       * |i_category|sales|min|
       * +----------+-----+---+
       * |c1        |3    |1  |
       * |c1        |1    |1  |
       * |c1        |9    |1  |
       * |c2        |2    |2  |
       * |c2        |20   |2  |
       * |c2        |5    |2  |
       * |c2        |5    |2  |
       * |c2        |4    |2  |
       * |c3        |1    |1  |
       * |c3        |9    |1  |
       * |c3        |7    |1  |
       * |c3        |11   |1  |
       * +----------+-----+---+
       *
       * AdaptiveSparkPlan isFinalPlan=false
       * +- Window [min(sales#92) windowspecdefinition(i_category#91, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS min#90], [i_category#91]
       * +- Sort [i_category#91 ASC NULLS FIRST], false, 0
       * +- Exchange hashpartitioning(i_category#91, 5), ENSURE_REQUIREMENTS, [plan_id=256]
       * +- Project [i_category#91, sales#92]
       * +- BatchScan dw2[i_category#91, sales#92] PaimonScan: [dw2] RuntimeFilters: []
       */

      val df4 = spark.sql(
        s"""
           |select
           |  i_category,
           |  sales,
           |  sum(sales) over (partition by i_category order by sales) as sum
           |from dw2
           |""".stripMargin)
      df4.show(false)
      // scalastyle:off println
      println(df4.queryExecution.executedPlan)
      // scalastyle:on println

      /**
       * +----------+-----+---+
       * |i_category|sales|sum|
       * +----------+-----+---+
       * |c1        |1    |1  |
       * |c1        |3    |4  |
       * |c1        |9    |13 |
       * |c2        |2    |2  |
       * |c2        |4    |6  |
       * |c2        |5    |16 |
       * |c2        |5    |16 |
       * |c2        |20   |36 |
       * |c3        |1    |1  |
       * |c3        |7    |8  |
       * |c3        |9    |17 |
       * |c3        |11   |28 |
       * +----------+-----+---+
       *
       * AdaptiveSparkPlan isFinalPlan=false
       * +- Window [sum(sales#119) windowspecdefinition(i_category#118, sales#119 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS sum#117L], [i_category#118], [sales#119 ASC NULLS FIRST]
       * +- Sort [i_category#118 ASC NULLS FIRST, sales#119 ASC NULLS FIRST], false, 0
       * +- Exchange hashpartitioning(i_category#118, 5), ENSURE_REQUIREMENTS, [plan_id=330]
       * +- Project [i_category#118, sales#119]
       * +- BatchScan dw2[i_category#118, sales#119] PaimonScan: [dw2] RuntimeFilters: []
       */

      val df5 = spark.sql(
        s"""
           |select
           |  i_category,
           |  sales,
           |  sum(sales) over (partition by i_category) as sum
           |from dw2
           |""".stripMargin)
      df5.show(false)
      // scalastyle:off println
      println(df5.queryExecution.executedPlan)
      // scalastyle:on println

      /**
       * +----------+-----+---+
       * |i_category|sales|sum|
       * +----------+-----+---+
       * |c1        |3    |13 |
       * |c1        |1    |13 |
       * |c1        |9    |13 |
       * |c2        |2    |36 |
       * |c2        |20   |36 |
       * |c2        |5    |36 |
       * |c2        |5    |36 |
       * |c2        |4    |36 |
       * |c3        |1    |28 |
       * |c3        |9    |28 |
       * |c3        |7    |28 |
       * |c3        |11   |28 |
       * +----------+-----+---+
       *
       * AdaptiveSparkPlan isFinalPlan=false
       * +- Window [sum(sales#146) windowspecdefinition(i_category#145, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sum#144L], [i_category#145]
       * +- Sort [i_category#145 ASC NULLS FIRST], false, 0
       * +- Exchange hashpartitioning(i_category#145, 5), ENSURE_REQUIREMENTS, [plan_id=404]
       * +- Project [i_category#145, sales#146]
       * +- BatchScan dw2[i_category#145, sales#146] PaimonScan: [dw2] RuntimeFilters: []
       */
    }
  }
}
