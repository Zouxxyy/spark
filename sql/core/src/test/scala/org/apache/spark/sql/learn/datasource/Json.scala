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

class Json extends BaseTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"USE spark_catalog")
  }

  test("Json: read files") {
    // spark will infer schema, see JsonInferSchema
    sql("create table t using json location " +
      "'/Users/zxy/project/emr/OneSpark/sql/core/src/test/resources/learn/datasource/json.text'")
    sql("desc table extended t").show(truncate = false)
    sql("show create table t").show(truncate = false)
    sql("select * from t").show()
  }

  test("Json: write and read") {
    withTable("t") {
      sql("create table t (address STRUCT<city: STRING, state: STRING>, name STRING) using json")
      sql(
        s"""
           |insert into t values
           |(named_struct('city', 'Columbus', 'state', 'Ohio'), 'Yin'),
           |(named_struct('city', 'a', 'state', 'California'), 'Michael'),
           |(named_struct('city', 'b', 'state', 'California'), 'Michael'),
           |(named_struct('city', 'c', 'state', 'California'), 'Michael'),
           |(named_struct('city', 'd', 'state', 'California'), 'Michael'),
           |(named_struct('city', 'e', 'state', 'California'), 'Michael'),
           |(named_struct('city', 'f', 'state', 'California'), 'Michael')
           |""".stripMargin)
      sql("select * from t").show()
    }
  }
}
