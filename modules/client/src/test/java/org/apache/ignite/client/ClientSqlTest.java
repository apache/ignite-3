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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.junit.jupiter.api.Test;

/**
 * SQL tests.
 */
@SuppressWarnings("resource")
public class ClientSqlTest extends AbstractClientTableTest {
    @Test
    public void testExecuteAsync() {
        AsyncResultSet<SqlRow> resultSet =  client.sql().executeAsync(null, "SELECT 1").join();

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());
        assertEquals(1, resultSet.currentPageSize());

        SqlRow row = resultSet.currentPage().iterator().next();
        assertEquals(1, row.intValue(0));
    }

    @Test
    public void testExecute() {
        ResultSet<SqlRow> resultSet =  client.sql().execute(null, "SELECT 1");

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

        AsyncResultSet<SqlRow> resultSet = client.sql().executeAsync(null, statement).join();

        Map<String, Object> props = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .collect(Collectors.toMap(x -> x.stringValue(0), x -> x.value(1)));

        assertEquals("SCHEMA2", props.get("schema"));
        assertEquals("124000", props.get("timeout"));
        assertEquals("235", props.get("pageSize"));
        assertEquals("Europe/London", props.get("timeZoneId"));
    }

    @Test
    public void testMetadata() {
        ResultSet<SqlRow> resultSet =  client.sql().execute(null, "SELECT META");
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

        assertEquals(BigDecimal.valueOf(145).setScale(2, RoundingMode.HALF_UP), row.value(7));
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

        ResultSet<SqlRow> resultSet = sql.execute(null, "SELECT LAST SCRIPT");
        SqlRow row = resultSet.next();

        assertEquals(
                "foo, arguments: [], defaultSchema=<not set>, defaultQueryTimeout=0",
                row.value(0));
    }

    @Test
    public void testExecuteScriptWithPropertiesAndArguments() {
        IgniteSql sql = client.sql();

        sql.executeScript("do bar baz", "arg1", null, 2);

        ResultSet<SqlRow> resultSet = sql.execute(null, "SELECT LAST SCRIPT");
        SqlRow row = resultSet.next();

        assertEquals(
                "do bar baz, arguments: [arg1, null, 2, ], defaultSchema=<not set>, defaultQueryTimeout=0",
                row.value(0));
    }
}
