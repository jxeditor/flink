/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.RowTypeTestBase
import org.apache.flink.table.planner.utils.DateTimeTestUtil.{localDate, localDateTime, localTime => gLocalTime}

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class RowTypeTest extends RowTypeTestBase {

  @Test
  def testRowLiteral(): Unit = {

    // primitive literal
    testAllApis(row(1, "foo", true), "ROW(1, 'foo', true)", "(1, foo, TRUE)")

    // special literal
    testTableApi(
      row(
        localDate("1985-04-11"),
        gLocalTime("14:15:16"),
        localDateTime("1985-04-11 14:15:16"),
        BigDecimal("0.1").bigDecimal,
        array(1, 2, 3),
        map("foo", "bar"),
        row(1, true)
      ),
      "(1985-04-11, 14:15:16, 1985-04-11 14:15:16, 0.1, [1, 2, 3], {foo=bar}, (1, TRUE))"
    )
    testSqlApi(
      "ROW(DATE '1985-04-11', TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16', " +
        "CAST(0.1 AS DECIMAL(2, 1)), ARRAY[1, 2, 3], MAP['foo', 'bar'], row(1, true))",
      "(1985-04-11, 14:15:16, 1985-04-11 14:15:16, 0.1, [1, 2, 3], {foo=bar}, (1, TRUE))"
    )

    testSqlApi(
      "ROW(DATE '1985-04-11', TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16.123456', " +
        "CAST(0.1 AS DECIMAL(2, 1)), ARRAY[1, 2, 3], MAP['foo', 'bar'], row(1, true))",
      "(1985-04-11, 14:15:16, 1985-04-11 14:15:16.123456, 0.1, [1, 2, 3], {foo=bar}, (1, TRUE))"
    )

    testAllApis(
      row(1 + 1, 2 * 3, nullOf(DataTypes.STRING())),
      "ROW(1 + 1, 2 * 3, NULLIF(1, 1))",
      "(2, 6, NULL)")

    testSqlApi("(1, 'foo', true)", "(1, foo, TRUE)")
  }

  @Test
  def testRowField(): Unit = {
    testAllApis(row('f0, 'f1), "(f0, f1)", "(NULL, 1)")

    testAllApis('f2, "f2", "(2, foo, TRUE)")

    testAllApis(row('f2, 'f5), "(f2, f5)", "((2, foo, TRUE), (foo, NULL))")

    testAllApis('f4, "f4", "(1984-03-12, 0.00000000, [1, 2, 3])")

    testAllApis(row('f1, "foo", true), "(f1, 'foo',true)", "(1, foo, TRUE)")
  }

  @Test
  def testRowOperations(): Unit = {
    testAllApis('f5.get("f0"), "f5.f0", "foo")

    testAllApis('f3.get("f1").get("f2"), "f3.f1.f2", "TRUE")

    // SQL API for row value constructor follow by field access is not supported
    testTableApi(row('f1, 'f6, 'f2).get("f1").get("f1"), "NULL")
  }

  @Test
  def testUnsupportedCastTableApi(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          testTableApi(
            'f5.cast(DataTypes.BIGINT()),
            ""
          ))
  }

  @Test
  def testUnsupportedCastSqlApi(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          testSqlApi(
            "CAST(f5 AS BIGINT)",
            ""
          ))
      .withMessageContaining("Cast function cannot convert value")
  }

  @Test
  def testRowTypeEquality(): Unit = {
    testAllApis('f2 === row(2, "foo", true), "f2 = row(2, 'foo', true)", "TRUE")

    testAllApis('f3 === row(3, row(2, "foo", true)), "f3 = row(3, row(2, 'foo', true))", "TRUE")
  }
}
