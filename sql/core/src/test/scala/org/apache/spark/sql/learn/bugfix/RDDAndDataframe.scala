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

package org.apache.spark.sql.learn.bugfix

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.learn.BaseTest

class RDDAndDataframe extends BaseTest {

  import testImplicits._

  test("RDD: test parallelize") {
    val tuples = spark.sparkContext.parallelize(1 to 1).map(x => (x, x)).collect()
    tuples
  }

  test("Dataframe: test repartitionByCol") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withSQLConf("spark.sql.shuffle.partitions" -> "40") {
        val df = (1 to 20).toDF("id")
        val partitions = df.rdd.getNumPartitions
        val df2 = df.repartition(col("id"))
        val partitions2 = df2.rdd.getNumPartitions
        df2.collect()
      }

      withSQLConf("spark.sql.shuffle.partitions" -> "10") {
        val df = (1 to 20).toDF("id")
        val partitions = df.rdd.getNumPartitions
        val df2 = df.repartition(col("id"))
        val partitions2 = df2.rdd.getNumPartitions
        df2.collect()
      }
    }
  }
}
