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

class DataType extends BaseTest {

  test("DataType: struct type") {
    withTable("students") {
      sql(
        """
          |CREATE TABLE students (
          |    name STRING,
          |    age INT,
          |    courses ARRAY<STRUCT<course_name: STRING, grade: DOUBLE>>
          |) USING paimon;
          |""".stripMargin)

      sql(
        """
          |INSERT INTO students VALUES
          |('Alice', 20, ARRAY(STRUCT('Math', 85.0), STRUCT('English', 88.0))),
          |('Bob', 22, ARRAY(STRUCT('Math', 90.0), STRUCT('Biology', 92.0))),
          |('Cathy', 21, ARRAY(STRUCT('History', 95.0)));
          |""".stripMargin)

      sql(
        """
          |SELECT
          |    name,
          |    age,
          |    course.course_name,
          |    course.grade
          |FROM
          |    students
          |LATERAL VIEW explode(courses) AS course;
          |""".stripMargin).show()
    }
  }

  test("DataType: struct type CTAS") {
    withTable("students") {
      sql(
        """
          |CREATE TABLE students AS
          |SELECT *
          |FROM (VALUES ('Alice', 20, ARRAY(STRUCT('Math', 85.0), STRUCT('English', 88.0))),
          |             ('Bob', 22, ARRAY(STRUCT('Math', 90.0), STRUCT('Biology', 92.0))),
          |             ('Cathy', 21, ARRAY(STRUCT('History', 95.0)))) ;
          |""".stripMargin)

      sql("desc table extended students").show(false)
    }
  }

  test("DataType: struct type get") {
    withTable("students") {
      sql(
        """
          |CREATE TABLE students (
          |    name STRING,
          |    age INT,
          |    course STRUCT<course_name: STRING, grade: DOUBLE>
          |) USING paimon;
          |""".stripMargin)

      sql(
        """
          |INSERT INTO students VALUES
          |('Alice', 20, STRUCT('Math', 85.0)),
          |('Bob', 22, STRUCT('Biology', 92.0)),
          |('Cathy', 21, STRUCT('History', 95.0));
          |""".stripMargin)

      sql(
        """
          |SELECT
          |    name,
          |    age,
          |    course.course_name,
          |    course.grade
          |FROM
          |    students;
          |""".stripMargin).show()
    }
  }
}
