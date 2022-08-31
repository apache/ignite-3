/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlColumnType;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.junit.jupiter.api.Test;

/**
 * SQL tests.
 */
public class ClientSqlTest extends AbstractClientTableTest {
    @Test
    public void testExecuteAsync() {
        Session session = client.sql().createSession();
        AsyncResultSet resultSet = session.executeAsync(null, "SELECT 1").join();

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());
        assertEquals(1, resultSet.currentPageSize());

        SqlRow row = resultSet.currentPage().iterator().next();
        assertEquals(1, row.intValue(0));
    }

    @Test
    public void testExecute() {
        Session session = client.sql().createSession();
        ResultSet resultSet = session.execute(null, "SELECT 1");

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());

        SqlRow row = resultSet.next();
        assertEquals(1, row.intValue(0));
    }

    @Test
    public void testSessionPropertiesPropagation() {
        Session session = client.sql().sessionBuilder()
                .defaultSchema("SCHEMA1")
                .defaultQueryTimeout(123, TimeUnit.SECONDS)
                .defaultPageSize(234)
                .property("prop1", "1")
                .property("prop2", "2")
                .build();

        AsyncResultSet resultSet = session.executeAsync(null, "SELECT PROPS").join();

        Map<String, Object> props = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .collect(Collectors.toMap(x -> x.stringValue(0), x -> x.value(1)));

        assertEquals("SCHEMA1", props.get("schema"));
        assertEquals("123000", props.get("timeout"));
        assertEquals("234", props.get("pageSize"));
        assertEquals("1", props.get("prop1"));
        assertEquals("2", props.get("prop2"));
    }

    @Test
    public void testStatementPropertiesOverrideSessionProperties() {
        Session session = client.sql().sessionBuilder()
                .defaultSchema("SCHEMA1")
                .defaultQueryTimeout(123, TimeUnit.SECONDS)
                .defaultPageSize(234)
                .property("prop1", "1")
                .property("prop2", "2")
                .build();

        Statement statement = client.sql().statementBuilder()
                .query("SELECT PROPS")
                .defaultSchema("SCHEMA2")
                .queryTimeout(124, TimeUnit.SECONDS)
                .pageSize(235)
                .property("prop2", "22")
                .property("prop3", "3")
                .build();

        AsyncResultSet resultSet = session.executeAsync(null, statement).join();

        Map<String, Object> props = StreamSupport.stream(resultSet.currentPage().spliterator(), false)
                .collect(Collectors.toMap(x -> x.stringValue(0), x -> x.value(1)));

        assertEquals("SCHEMA2", props.get("schema"));
        assertEquals("124000", props.get("timeout"));
        assertEquals("235", props.get("pageSize"));
        assertEquals("1", props.get("prop1"));
        assertEquals("22", props.get("prop2"));
        assertEquals("3", props.get("prop3"));
    }

    @Test
    public void testMetadata() {
        Session session = client.sql().createSession();
        ResultSet resultSet = session.execute(null, "SELECT META");
        ResultSetMetadata meta = resultSet.metadata();
        SqlRow row = resultSet.next();

        assertNotNull(meta);
        assertSame(meta, row.metadata());

        for (int i = 0; i < meta.columns().size(); i++) {
            ColumnMetadata col = meta.columns().get(i);
            assertEquals(i, meta.indexOf(col.name()));
            assertEquals(row.<Object>value(i), row.value(col.name()));
        }

        assertTrue((boolean) row.value(0));
        assertEquals(SqlColumnType.BOOLEAN, meta.columns().get(0).type());

        assertEquals(Byte.MIN_VALUE, row.byteValue(1));
        assertEquals(SqlColumnType.INT8, meta.columns().get(1).type());

        assertEquals(Short.MIN_VALUE, row.shortValue(2));
        assertEquals(SqlColumnType.INT16, meta.columns().get(2).type());

        assertEquals(Integer.MIN_VALUE, row.intValue(3));
        assertEquals(SqlColumnType.INT32, meta.columns().get(3).type());

        assertEquals(Long.MIN_VALUE, row.longValue(4));
        assertEquals(SqlColumnType.INT64, meta.columns().get(4).type());

        assertEquals(1.3f, row.floatValue(5));
        assertEquals(SqlColumnType.FLOAT, meta.columns().get(5).type());

        assertEquals(1.4d, row.doubleValue(6));
        assertEquals(SqlColumnType.DOUBLE, meta.columns().get(6).type());

        assertEquals(BigDecimal.valueOf(145), row.value(7));
        ColumnMetadata decimalCol = meta.columns().get(7);
        assertEquals(SqlColumnType.DECIMAL, decimalCol.type());
        assertEquals(1, decimalCol.precision());
        assertEquals(2, decimalCol.scale());
        assertTrue(decimalCol.nullable());
        assertNotNull(decimalCol.origin());
        assertEquals("SCHEMA1", decimalCol.origin().schemaName());
        assertEquals("TBL2", decimalCol.origin().tableName());
        assertEquals("BIG_DECIMAL", decimalCol.origin().columnName());

        assertEquals(LocalDate.of(2001, 2, 3), row.dateValue(8));
        assertEquals(SqlColumnType.DATE, meta.columns().get(8).type());

        assertEquals(LocalTime.of(4, 5), row.timeValue(9));
        assertEquals(SqlColumnType.TIME, meta.columns().get(9).type());

        assertEquals(LocalDateTime.of(2001, 3, 4, 5, 6), row.datetimeValue(10));
        assertEquals(SqlColumnType.DATETIME, meta.columns().get(10).type());

        assertEquals(Instant.ofEpochSecond(987), row.timestampValue(11));
        assertEquals(SqlColumnType.TIMESTAMP, meta.columns().get(11).type());

        assertEquals(new UUID(0, 0), row.uuidValue(12));
        assertEquals(SqlColumnType.UUID, meta.columns().get(12).type());

        assertEquals(BitSet.valueOf(new byte[0]), row.bitmaskValue(13));
        assertEquals(SqlColumnType.BITMASK, meta.columns().get(13).type());

        assertEquals(0, ((byte[]) row.value(14))[0]);
        assertEquals(SqlColumnType.BYTE_ARRAY, meta.columns().get(14).type());

        assertEquals(Period.of(10, 9, 8), row.value(15));
        assertEquals(SqlColumnType.PERIOD, meta.columns().get(15).type());

        assertEquals(Duration.ofDays(11), row.value(16));
        assertEquals(SqlColumnType.DURATION, meta.columns().get(16).type());

        assertEquals(BigInteger.valueOf(42), row.value(17));
        assertEquals(SqlColumnType.NUMBER, meta.columns().get(17).type());
    }
}
