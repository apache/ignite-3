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

package org.apache.ignite.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.client.sql.ClientDirectTxMode;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.sql.PartitionMappingProvider;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * SQL tests.
 */
@SuppressWarnings("resource")
public class ClientSqlTest extends AbstractClientTableTest {
    @Test
    public void testExecuteAsync() {
        AsyncResultSet<SqlRow> resultSet =  client.sql().executeAsync("SELECT 1").join();

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());
        assertEquals(1, resultSet.currentPageSize());

        SqlRow row = resultSet.currentPage().iterator().next();
        assertEquals(1, row.intValue(0));
    }

    @Test
    public void testExecute() {
        ResultSet<SqlRow> resultSet =  client.sql().execute("SELECT 1");

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());

        SqlRow row = resultSet.next();
        assertEquals(1, row.intValue(0));
    }

    @Test
    public void testStatementPropertiesPropagation() {
        Statement statement = client.sql().statementBuilder()
                .query("SELECT PROPS")
                .defaultSchema("SCHEMA2")
                .queryTimeout(124, TimeUnit.SECONDS)
                .pageSize(235)
                .timeZoneId(ZoneId.of("Europe/London"))
                .build();

        AsyncResultSet<SqlRow> resultSet = client.sql().executeAsync((Transaction) null, statement).join();

        Map<String, Object> props = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .collect(Collectors.toMap(x -> x.stringValue(0), x -> x.value(1)));

        assertEquals("SCHEMA2", props.get("schema"));
        assertEquals("124000", props.get("timeout"));
        assertEquals("235", props.get("pageSize"));
        assertEquals("Europe/London", props.get("timeZoneId"));
    }

    @Test
    public void testMetadata() {
        ResultSet<SqlRow> resultSet =  client.sql().execute("SELECT META");
        ResultSetMetadata meta = resultSet.metadata();
        SqlRow row = resultSet.next();

        assertNotNull(meta);
        assertSame(meta, row.metadata());

        for (int i = 0; i < meta.columns().size(); i++) {
            ColumnMetadata col = meta.columns().get(i);
            assertEquals(i, meta.indexOf(col.name()));

            if (col.type() == ColumnType.BYTE_ARRAY) {
                assertArrayEquals(row.<byte[]>value(i), row.value(col.name()));
            } else {
                assertEquals(row.<Object>value(i), row.value(col.name()));
            }
        }

        assertTrue((boolean) row.value(0));
        assertEquals(ColumnType.BOOLEAN, meta.columns().get(0).type());

        assertEquals(Byte.MIN_VALUE, row.byteValue(1));
        assertEquals(ColumnType.INT8, meta.columns().get(1).type());

        assertEquals(Short.MIN_VALUE, row.shortValue(2));
        assertEquals(ColumnType.INT16, meta.columns().get(2).type());

        assertEquals(Integer.MIN_VALUE, row.intValue(3));
        assertEquals(ColumnType.INT32, meta.columns().get(3).type());

        assertEquals(Long.MIN_VALUE, row.longValue(4));
        assertEquals(ColumnType.INT64, meta.columns().get(4).type());

        assertEquals(1.3f, row.floatValue(5));
        assertEquals(ColumnType.FLOAT, meta.columns().get(5).type());

        assertEquals(1.4d, row.doubleValue(6));
        assertEquals(ColumnType.DOUBLE, meta.columns().get(6).type());

        assertDecimalEqual(BigDecimal.valueOf(145), row.decimalValue(7));
        ColumnMetadata decimalCol = meta.columns().get(7);
        assertEquals(ColumnType.DECIMAL, decimalCol.type());
        assertEquals(1, decimalCol.precision());
        assertEquals(2, decimalCol.scale());
        assertTrue(decimalCol.nullable());
        assertNotNull(decimalCol.origin());
        assertEquals("SCHEMA1", decimalCol.origin().schemaName());
        assertEquals("TBL2", decimalCol.origin().tableName());
        assertEquals("BIG_DECIMAL", decimalCol.origin().columnName());

        assertEquals(LocalDate.of(2001, 2, 3), row.dateValue(8));
        assertEquals(ColumnType.DATE, meta.columns().get(8).type());

        assertEquals(LocalTime.of(4, 5), row.timeValue(9));
        assertEquals(ColumnType.TIME, meta.columns().get(9).type());

        assertEquals(LocalDateTime.of(2001, 3, 4, 5, 6), row.datetimeValue(10));
        assertEquals(ColumnType.DATETIME, meta.columns().get(10).type());

        assertEquals(Instant.ofEpochSecond(987), row.timestampValue(11));
        assertEquals(ColumnType.TIMESTAMP, meta.columns().get(11).type());

        assertEquals(new UUID(0, 0), row.uuidValue(12));
        assertEquals(ColumnType.UUID, meta.columns().get(12).type());

        assertEquals(0, ((byte[]) row.value(13))[0]);
        assertEquals(ColumnType.BYTE_ARRAY, meta.columns().get(13).type());

        assertEquals(Period.of(10, 9, 8), row.value(14));
        assertEquals(ColumnType.PERIOD, meta.columns().get(14).type());

        assertEquals(Duration.ofDays(11), row.value(15));
        assertEquals(ColumnType.DURATION, meta.columns().get(15).type());
    }

    @Test
    public void testExecuteScript() {
        IgniteSql sql = client.sql();

        sql.executeScript("foo");

        ResultSet<SqlRow> resultSet = sql.execute("SELECT LAST SCRIPT");
        SqlRow row = resultSet.next();

        assertEquals(
                "foo, arguments: [], defaultSchema=PUBLIC, defaultQueryTimeout=0",
                row.value(0));
    }

    @Test
    public void testExecuteScriptWithPropertiesAndArguments() {
        IgniteSql sql = client.sql();

        sql.executeScript("do bar baz", "arg1", null, 2);

        ResultSet<SqlRow> resultSet = sql.execute("SELECT LAST SCRIPT");
        SqlRow row = resultSet.next();

        assertEquals(
                "do bar baz, arguments: [arg1, null, 2, ], defaultSchema=PUBLIC, defaultQueryTimeout=0",
                row.value(0));
    }

    @Test
    void partitionAwarenessMetas() {
        IgniteSql sql = client.sql();

        Statement statement1 = sql.statementBuilder()
                .query("SELECT PA")
                .defaultSchema("SCHEMA_1")
                .build();
        Statement statement2 = sql.statementBuilder()
                .query("SELECT PA")
                .defaultSchema("SCHEMA_2")
                .build();

        sql.execute((Transaction) null, statement1);
        sql.execute((Transaction) null, statement2);

        List<PartitionMappingProvider> metas = ((ClientSql) sql).partitionAwarenessCachedMetas();
        assertThat(metas.size(), CoreMatchers.is(2));

        for (PartitionMappingProvider meta : metas) {
            assertThat(meta.tableId(), CoreMatchers.is(1));
            assertThat(meta.indexes(), CoreMatchers.is(new int[] {0, -1, -2, 2}));
            assertThat(meta.hash(), CoreMatchers.is(new int[] {100, 500}));
            assertThat(meta.directTxMode(), CoreMatchers.is(ClientDirectTxMode.SUPPORTED_TRACKING_REQUIRED));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 8, 16})
    void partitionAwarenessMetadataCacheOverflow(int size) throws InterruptedException {
        ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE, 1);

        try (IgniteClient client = createClientWithPaCacheOfSize(size)) {
            IgniteSql sql = client.sql();

            // use (size + 1) to account for size=0 case
            for (int i = 0; i < 2 * (size + 1); i++) {
                Statement statement1 = sql.statementBuilder()
                        .query("SELECT PA")
                        .defaultSchema("SCHEMA_" + i)
                        .build();

                sql.execute((Transaction) null, statement1);
            }

            assertTrue(waitForCondition(
                    () -> ((ClientSql) sql).partitionAwarenessCachedMetas().size() <= size, 5_000
            ));
        }
    }

    @ParameterizedTest(name = "{0} => {1}")
    @MethodSource("testQueryModifiersArgs")
    void testQueryModifiers(QueryModifier modifier, String expectedQueryTypes) {
        IgniteSql sql = client.sql();

        AsyncResultSet<SqlRow> results = await(((ClientSql) sql).executeAsyncInternal(
                null, null, null, Set.of(modifier), sql.createStatement("SELECT ALLOWED QUERY TYPES")));

        assertTrue(results.hasRowSet());

        SqlRow row = results.currentPage().iterator().next();

        assertThat(row.stringValue(0), equalTo(expectedQueryTypes));
    }

    private static List<Arguments> testQueryModifiersArgs() {
        List<Arguments> res = new ArrayList<>();

        for (QueryModifier modifier : QueryModifier.values()) {
            String expected;

            switch (modifier) {
                case ALLOW_ROW_SET_RESULT:
                    expected = "EXPLAIN, QUERY";
                    break;

                case ALLOW_AFFECTED_ROWS_RESULT:
                    expected = "DML";
                    break;

                case ALLOW_APPLIED_RESULT:
                    expected = "DDL, KILL";
                    break;

                case ALLOW_TX_CONTROL:
                    expected = "TX_CONTROL";
                    break;

                case ALLOW_MULTISTATEMENT:
                    expected = "MULTISTATEMENT";
                    break;

                default:
                    throw new IllegalArgumentException("Unexpected type: " + modifier);
            }

            res.add(Arguments.of(modifier, expected));
        }

        return res;
    }

    private static IgniteClient createClientWithPaCacheOfSize(int cacheSize) {
        var builder = IgniteClient.builder()
                .addresses(new String[]{"127.0.0.1:" + serverPort})
                .sqlPartitionAwarenessMetadataCacheSize(cacheSize);

        return builder.build();
    }
}
