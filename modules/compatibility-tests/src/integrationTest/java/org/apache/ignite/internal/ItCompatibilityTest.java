/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

@ParameterizedClass
@MethodSource("baseVersions")
class ItCompatibilityTest extends CompatibilityTestBase {

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration("ignite.eventlog.sinks {"
                + "  logSink {"
                + "    channel: testChannel,"
                + "    type: log"
                + "  },"
                + "  webhookSink {"
                + "    channel: testChannel,"
                + "    type: webhook,"
                + "    endpoint: \"http://localhost\","
                + "  }"
                + "}");
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        sql(baseIgnite, "CREATE TABLE TEST_ALL_TYPES("
                + "ID INT PRIMARY KEY, "
                + "VAL_INT INT, "
                + "VAL_BIGINT BIGINT, "
                + "VAL_FLOAT FLOAT, "
                + "VAL_DOUBLE DOUBLE, "
                + "VAL_DECIMAL DECIMAL(10,2), "
                + "VAL_BOOL BOOLEAN, "
                + "VAL_STR VARCHAR, "
                + "VAL_DATE DATE, "
                + "VAL_TIME TIME, "
                + "VAL_TIMESTAMP TIMESTAMP)");

        baseIgnite.transactions().runInTransaction(tx -> {
            sql(baseIgnite, tx, "INSERT INTO TEST_ALL_TYPES VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                    "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56), LocalDateTime.of(2025, 7, 23, 12, 34, 56));
        });

        List<List<Object>> result = sql(baseIgnite, "SELECT * FROM TEST_ALL_TYPES");
        assertThat(result, contains(contains(
                1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56), LocalDateTime.of(2025, 7, 23, 12, 34, 56)
        )));
    }

    @Test
    void testAllTypesAndTransactions() {
        // Read old data
        List<List<Object>> result = sql("SELECT * FROM TEST_ALL_TYPES WHERE ID = 1");
        assertThat(result, contains(contains(
                1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56), LocalDateTime.of(2025, 7, 23, 12, 34, 56)
        )));

        // Insert new data using transaction
        node(0).transactions().runInTransaction(tx -> {
            sql(node(0), tx, "INSERT INTO TEST_ALL_TYPES VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    2, -1, -9876543210L, 0.0f, -1.23d, new BigDecimal("0.00"), false,
                    "world", LocalDate.of(2020, 1, 1), LocalTime.of(0, 0, 0), LocalDateTime.of(2020, 1, 1, 0, 0, 0));
        });

        // Verify all data
        result = sql("SELECT * FROM TEST_ALL_TYPES");
        assertThat(result, containsInAnyOrder(
                contains(
                        1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                        "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56), LocalDateTime.of(2025, 7, 23, 12, 34, 56)
                ),
                contains(
                        2, -1, -9876543210L, 0.0f, -1.23d, new BigDecimal("0.00"), false,
                        "world", LocalDate.of(2020, 1, 1), LocalTime.of(0, 0, 0), LocalDateTime.of(2020, 1, 1, 0, 0, 0)
                )
        ));
    }
}
