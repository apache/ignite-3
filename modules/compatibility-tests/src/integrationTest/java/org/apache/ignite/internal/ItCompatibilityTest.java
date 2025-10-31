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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

@ParameterizedClass(name = "{displayName}({argumentsWithNames})")
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
                + "VAL_TIMESTAMP_LOCAL TIMESTAMP WITH LOCAL TIME ZONE, "
                + "VAL_TIMESTAMP TIMESTAMP)");

        baseIgnite.transactions().runInTransaction(tx -> {
            sql(baseIgnite, tx, "INSERT INTO TEST_ALL_TYPES VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                    "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56),
                    Instant.parse("2025-05-05T02:05:05Z"), LocalDateTime.of(2025, 7, 23, 12, 34, 56));
        });

        List<List<Object>> result = sql(baseIgnite, "SELECT * FROM TEST_ALL_TYPES");
        assertThat(result, contains(contains(
                1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56),
                Instant.parse("2025-05-05T02:05:05Z"), LocalDateTime.of(2025, 7, 23, 12, 34, 56)
        )));

        // Check default values
        sql(baseIgnite, "CREATE TABLE TEST_DEFAULT("
                + "ID INT DEFAULT 1 PRIMARY KEY, "
                + "VAL_INT INT DEFAULT 2, "
                + "VAL_BIGINT BIGINT DEFAULT 3, "
                + "VAL_FLOAT FLOAT DEFAULT 4.0, "
                + "VAL_DOUBLE DOUBLE DEFAULT 5.0, "
                + "VAL_DECIMAL DECIMAL(10,2) DEFAULT 6.00, "
                + "VAL_BOOL BOOLEAN DEFAULT FALSE, "
                + "VAL_STR VARCHAR DEFAULT 'test', "
                + "VAL_DATE DATE DEFAULT DATE '1970-01-02', "
                + "VAL_TIME TIME DEFAULT TIME '00:00:01', "
                + "VAL_TIMESTAMP TIMESTAMP DEFAULT TIMESTAMP '1970-01-31 00:00:31'"
                + ")");

        baseIgnite.transactions().runInTransaction(tx -> {
            sql(baseIgnite, tx, "INSERT INTO TEST_DEFAULT VALUES (DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, "
                    + "DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)");
        });
        result = sql(baseIgnite, "SELECT * FROM TEST_DEFAULT");
        assertThat(result, contains(contains(
                1, 2, 3L, 4.0f, 5.0d, new BigDecimal("6.00"), false,
                "test", LocalDate.of(1970, 1, 2), LocalTime.of(0, 0, 01), LocalDateTime.of(1970, 1, 31, 0, 0, 31)
        )));
    }

    @Test
    void testCompatibility() {
        // Read old data
        List<List<Object>> result = sql("SELECT * FROM TEST_ALL_TYPES");
        assertThat(result, contains(contains(
                1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56),
                Instant.parse("2025-05-05T02:05:05Z"), LocalDateTime.of(2025, 7, 23, 12, 34, 56)
        )));

        // Read default data
        List<List<Object>> resultDefault = sql("SELECT * FROM TEST_DEFAULT");
        assertThat(resultDefault, contains(contains(
                1, 2, 3L, 4.0f, 5.0d, new BigDecimal("6.00"), false,
                "test", LocalDate.of(1970, 1, 2), LocalTime.of(0, 0, 01), LocalDateTime.of(1970, 1, 31, 0, 0, 31)
        )));

        // Insert new data using transaction
        node(0).transactions().runInTransaction(tx -> {
            sql(node(0), tx, "INSERT INTO TEST_ALL_TYPES VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    2, -1, -9876543210L, 0.0f, -1.23d, new BigDecimal("0.00"), false,
                    "world", LocalDate.of(2020, 1, 1), LocalTime.of(0, 0, 0),
                    Instant.parse("2020-05-05T02:05:05Z"), LocalDateTime.of(2020, 1, 1, 0, 0, 0));
        });

        // Verify all data
        result = sql("SELECT * FROM TEST_ALL_TYPES");
        assertThat(result, containsInAnyOrder(
                contains(
                        1, 42, 1234567890123L, 3.14f, 2.71828d, new BigDecimal("1234.56"), true,
                        "hello", LocalDate.of(2025, 7, 23), LocalTime.of(12, 34, 56),
                        Instant.parse("2025-05-05T02:05:05Z"), LocalDateTime.of(2025, 7, 23, 12, 34, 56)
                ),
                contains(
                        2, -1, -9876543210L, 0.0f, -1.23d, new BigDecimal("0.00"), false,
                        "world", LocalDate.of(2020, 1, 1), LocalTime.of(0, 0, 0),
                        Instant.parse("2020-05-05T02:05:05Z"), LocalDateTime.of(2020, 1, 1, 0, 0, 0)
                )
        ));
    }
}
