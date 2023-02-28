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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.runtime.CalciteContextException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Test SQL data types.
 */
public class ItDataTypesTest extends AbstractBasicIntegrationTest {
    /**
     * Drops all created tables.
     */
    @AfterEach
    public void dropTables() {
        var igniteTables = CLUSTER_NODES.get(0).tables();

        for (var table : igniteTables.tables()) {
            sql("DROP TABLE " + table.name());
        }
    }

    /** Tests correctness with unicode. */
    @Test
    public void testUnicodeStrings() {
        sql("CREATE TABLE string_table(key int primary key, val varchar)");

        String[] values = new String[]{"Кирилл", "Müller", "我是谁", "ASCII"};

        int key = 0;

        // Insert as inlined values.
        for (String val : values) {
            sql("INSERT INTO string_table (key, val) VALUES (?, ?)", key++, val);
        }

        var rows = sql("SELECT val FROM string_table");

        assertEquals(Set.of(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        sql("DELETE FROM string_table");

        // Insert as parameters.
        for (String val : values) {
            sql("INSERT INTO string_table (key, val) VALUES (?, ?)", key++, val);
        }

        rows = sql("SELECT val FROM string_table");

        assertEquals(Set.of(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        rows = sql("SELECT substring(val, 1, 2) FROM string_table");

        assertEquals(Set.of("Ки", "Mü", "我是", "AS"),
                rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        for (String val : values) {
            rows = sql("SELECT char_length(val) FROM string_table WHERE val = ?", val);

            assertEquals(1, rows.size());
            assertEquals(val.length(), rows.get(0).get(0));
        }
    }

    /** Tests NOT NULL and DEFAULT column constraints. */
    @Test
    public void testCheckDefaultsAndNullables() {
        sql("CREATE TABLE tbl(c1 int PRIMARY KEY, c2 int NOT NULL, c3 int NOT NULL DEFAULT 100)");

        sql("INSERT INTO tbl(c1, c2) VALUES (1, 2)");

        var rows = sql("SELECT c3 FROM tbl");

        assertEquals(Set.of(100), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        sql("ALTER TABLE tbl ADD COLUMN c4 int NOT NULL DEFAULT 101");

        rows = sql("SELECT c4 FROM tbl");

        assertEquals(Set.of(101), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        //todo: correct exception https://issues.apache.org/jira/browse/IGNITE-16095
        assertThrows(CalciteContextException.class, () -> sql("INSERT INTO tbl(c1, c2) VALUES (2, NULL)"));
    }

    /**
     * Tests numeric types mapping on Java types.
     */
    @Test
    public void testNumericRanges() {
        sql("CREATE TABLE tbl(id int PRIMARY KEY, tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT)");

        sql("INSERT INTO tbl VALUES (1, " + Byte.MAX_VALUE + ", " + Short.MAX_VALUE + ", "
                + Integer.MAX_VALUE + ", " + Long.MAX_VALUE + ')');

        assertQuery("SELECT tiny FROM tbl").returns(Byte.MAX_VALUE).check();
        assertQuery("SELECT small FROM tbl").returns(Short.MAX_VALUE).check();
        assertQuery("SELECT i FROM tbl").returns(Integer.MAX_VALUE).check();
        assertQuery("SELECT big FROM tbl").returns(Long.MAX_VALUE).check();

        sql("DELETE from tbl");

        sql("INSERT INTO tbl VALUES (1, " + Byte.MIN_VALUE + ", " + Short.MIN_VALUE + ", "
                + Integer.MIN_VALUE + ", " + Long.MIN_VALUE + ')');

        assertQuery("SELECT tiny FROM tbl").returns(Byte.MIN_VALUE).check();
        assertQuery("SELECT small FROM tbl").returns(Short.MIN_VALUE).check();
        assertQuery("SELECT i FROM tbl").returns(Integer.MIN_VALUE).check();
        assertQuery("SELECT big FROM tbl").returns(Long.MIN_VALUE).check();
    }

    /**
     * Tests numeric type convertation on equals.
     */
    @Test
    public void testNumericConvertingOnEquals() {
        sql("CREATE TABLE tbl(id int PRIMARY KEY, tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT)");

        sql("INSERT INTO tbl VALUES (-1, 1, 2, 3, 4), (0, 5, 5, 5, 5)");

        assertQuery("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.small)").returns((byte) 5).check();
        assertQuery("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.tiny)").returns((short) 5).check();

        assertQuery("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.i)").returns((byte) 5).check();
        assertQuery("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.tiny)").returns(5).check();

        assertQuery("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.big)").returns((byte) 5).check();
        assertQuery("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.tiny)").returns(5L).check();

        assertQuery("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.i)").returns((short) 5).check();
        assertQuery("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.small)").returns(5).check();

        assertQuery("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.big)").returns((short) 5).check();
        assertQuery("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.small)").returns(5L).check();

        assertQuery("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.big)").returns(5).check();
        assertQuery("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.i)").returns(5L).check();
    }

    /**
     * Test right date/time interpretation.
     */
    @Test
    public void testDateTime() {
        assertQuery("select date '1992-01-19'").returns(sqlDate("1992-01-19")).check();
        assertQuery("select date '1992-01-18' + interval (1) days").returns(sqlDate("1992-01-19")).check();
        assertQuery("select date '1992-01-18' + interval (24) hours").returns(sqlDate("1992-01-19")).check();
        assertQuery("SELECT timestamp '1992-01-18 02:30:00' + interval (25) hours")
                .returns(sqlDateTime("1992-01-19T03:30:00")).check();
        assertQuery("SELECT timestamp '1992-01-18 02:30:00' + interval (23) hours")
                .returns(sqlDateTime("1992-01-19T01:30:00.000")).check();
        assertQuery("SELECT timestamp '1992-01-18 02:30:00' + interval (24) hours")
                .returns(sqlDateTime("1992-01-19T02:30:00.000")).check();

        assertQuery("select date '1992-03-29'").returns(sqlDate("1992-03-29")).check();
        assertQuery("select date '1992-03-28' + interval (1) days").returns(sqlDate("1992-03-29")).check();
        assertQuery("select date '1992-03-28' + interval (24) hours").returns(sqlDate("1992-03-29")).check();
        assertQuery("SELECT timestamp '1992-03-28 02:30:00' + interval (25) hours")
                .returns(sqlDateTime("1992-03-29T03:30:00.000")).check();
        assertQuery("SELECT timestamp '1992-03-28 02:30:00' + interval (23) hours")
                .returns(sqlDateTime("1992-03-29T01:30:00.000")).check();
        assertQuery("SELECT timestamp '1992-03-28 02:30:00' + interval (24) hours")
                .returns(sqlDateTime("1992-03-29T02:30:00.000")).check();

        assertQuery("select date '1992-09-27'").returns(sqlDate("1992-09-27")).check();
        assertQuery("select date '1992-09-26' + interval (1) days").returns(sqlDate("1992-09-27")).check();
        assertQuery("select date '1992-09-26' + interval (24) hours").returns(sqlDate("1992-09-27")).check();
        assertQuery("SELECT timestamp '1992-09-26 02:30:00' + interval (25) hours")
                .returns(sqlDateTime("1992-09-27T03:30:00.000")).check();
        assertQuery("SELECT timestamp '1992-09-26 02:30:00' + interval (23) hours")
                .returns(sqlDateTime("1992-09-27T01:30:00.000")).check();
        assertQuery("SELECT timestamp '1992-09-26 02:30:00' + interval (24) hours")
                .returns(sqlDateTime("1992-09-27T02:30:00.000")).check();

        assertQuery("select date '2021-11-07'").returns(sqlDate("2021-11-07")).check();
        assertQuery("select date '2021-11-06' + interval (1) days").returns(sqlDate("2021-11-07")).check();
        assertQuery("select date '2021-11-06' + interval (24) hours").returns(sqlDate("2021-11-07")).check();
        assertQuery("SELECT timestamp '2021-11-06 01:30:00' + interval (25) hours")
                .returns(sqlDateTime("2021-11-07T02:30:00.000")).check();
        // Check string representation here, since after timestamp calculation we have '2021-11-07T01:30:00.000-0800'
        // but Timestamp.valueOf method converts '2021-11-07 01:30:00' in 'America/Los_Angeles' time zone to
        // '2021-11-07T01:30:00.000-0700' (we pass through '2021-11-07 01:30:00' twice after DST ended).
        assertQuery("SELECT (timestamp '2021-11-06 02:30:00' + interval (23) hours)::varchar")
                .returns("2021-11-07 01:30:00").check();
        assertQuery("SELECT (timestamp '2021-11-06 01:30:00' + interval (24) hours)::varchar")
                .returns("2021-11-07 01:30:00").check();
    }

    /** Test Binary type. */
    @Test
    public void testBinarySql() {
        sql("CREATE TABLE tbl(b BINARY(3) PRIMARY KEY, v VARBINARY)");
        byte[] val = {1, 2, 3};

        // From parameters to internal, from internal to store, from store to internal and from internal to user.
        sql("INSERT INTO tbl VALUES (?, ?)", val, val);

        List<List<Object>> res = sql("SELECT b, v FROM tbl");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
        assertTrue(Objects.deepEquals(val, res.get(0).get(0)));
        assertTrue(Objects.deepEquals(val, res.get(0).get(1)));

        sql("DELETE FROM tbl");

        // From literal to internal, from internal to store, from store to internal and from internal to user.
        sql("INSERT INTO tbl VALUES (x'1A2B3C', x'AABBCC')");

        res = sql("SELECT b, v FROM tbl");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
        assertTrue(Objects.deepEquals(new byte[]{0x1A, 0x2B, 0x3C}, res.get(0).get(0)));
        assertTrue(Objects.deepEquals(new byte[]{(byte) 0xAA, (byte) 0xBB, (byte) 0xCC}, res.get(0).get(1)));
    }

    /** Test Aggregation functions for Binary type. */
    @Test
    public void testBinaryAggregation() {
        sql("CREATE TABLE tbl(p int PRIMARY KEY,b varbinary)");
        sql("INSERT INTO tbl VALUES (1, NULL)");
        sql("INSERT INTO tbl VALUES (2, x'010203')");
        sql("INSERT INTO tbl VALUES (3, x'040506')");
        List<List<Object>> res = sql("SELECT MIN(b), MAX(b) FROM tbl");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
        assertTrue(Objects.deepEquals(new byte[]{1, 2, 3}, res.get(0).get(0)));
        assertTrue(Objects.deepEquals(new byte[]{4, 5, 6}, res.get(0).get(1)));

        sql("INSERT INTO tbl VALUES (4, x'01'),(5, x'FF'), (6, x'99aabbccddeeff00')");
        res = sql("SELECT MIN(b), MAX(b) FROM tbl");

        assertTrue(Objects.deepEquals(new byte[]{1}, res.get(0).get(0)));
        assertTrue(Objects.deepEquals(new byte[]{-1}, res.get(0).get(1)));

        sql("INSERT INTO tbl VALUES (7, x'FFFF')");
        res = sql("SELECT MAX(b) FROM tbl");

        assertTrue(Objects.deepEquals(new byte[]{-1, -1}, res.get(0).get(0)));
    }

    /** Test concatenation for Binary type. */
    @Test
    public void testBinaryConcat() {
        sql("CREATE TABLE tbl(p int PRIMARY KEY, b varbinary)");
        sql("INSERT INTO tbl VALUES (1, x'010203')");
        List<List<Object>> res = sql("SELECT b || x'040506' FROM tbl");

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertTrue(Objects.deepEquals(new byte[]{1, 2, 3, 4, 5, 6}, res.get(0).get(0)));
    }

    /** Test decimal scale for dynamic parameters. */
    @Test
    public void testDecimalScale() {
        sql("CREATE TABLE t (id INT PRIMARY KEY, val1 DECIMAL(5, 3), val2 DECIMAL(3), val3 DECIMAL)");

        // Check literals scale.
        sql("INSERT INTO t values (0, 0, 0, 0)");
        sql("INSERT INTO t values (1.1, 1.1, 1.1, 1.1)");
        sql("INSERT INTO t values (2.123, 2.123, 2.123, 2.123)");
        sql("INSERT INTO t values (3.123456, 3.123456, 3.123456, 3.123456)");

        // Check dynamic parameters scale.
        List<Number> params = List.of(4, 5L, 6f, 7.25f, 8d, 9.03125d, new BigDecimal("10"),
                new BigDecimal("11.1"), new BigDecimal("12.123456"));

        for (Object val : params) {
            sql("INSERT INTO t values (?, ?, ?, ?)", val, val, val, val);
        }

        assertQuery("SELECT * FROM t")
                .returns(0, new BigDecimal("0.000"), new BigDecimal("0"), new BigDecimal("0"))
                .returns(1, new BigDecimal("1.100"), new BigDecimal("1"), new BigDecimal("1"))
                .returns(2, new BigDecimal("2.123"), new BigDecimal("2"), new BigDecimal("2"))
                .returns(3, new BigDecimal("3.123"), new BigDecimal("3"), new BigDecimal("3"))
                .returns(4, new BigDecimal("4.000"), new BigDecimal("4"), new BigDecimal("4"))
                .returns(5, new BigDecimal("5.000"), new BigDecimal("5"), new BigDecimal("5"))
                .returns(6, new BigDecimal("6.000"), new BigDecimal("6"), new BigDecimal("6"))
                .returns(7, new BigDecimal("7.250"), new BigDecimal("7"), new BigDecimal("7"))
                .returns(8, new BigDecimal("8.000"), new BigDecimal("8"), new BigDecimal("8"))
                .returns(9, new BigDecimal("9.031"), new BigDecimal("9"), new BigDecimal("9"))
                .returns(10, new BigDecimal("10.000"), new BigDecimal("10"), new BigDecimal("10"))
                .returns(11, new BigDecimal("11.100"), new BigDecimal("11"), new BigDecimal("11"))
                .returns(12, new BigDecimal("12.123"), new BigDecimal("12"), new BigDecimal("12"))
                .check();
    }

    /** Tests conversion between numeric types. */
    @Test
    public void testNumericConversion() {
        sql("CREATE TABLE t (v1 TINYINT PRIMARY KEY, v2 SMALLINT, v3 INT, v4 BIGINT, v5 DECIMAL, v6 FLOAT, v7 DOUBLE)");

        List<Number> params = List.of((byte) 1, (short) 2, 3, 4L, BigDecimal.valueOf(5), 6f, 7d);

        for (Object val : params) {
            sql("INSERT INTO t values (?, ?, ?, ?, ?, ?, ?)", val, val, val, val, val, val, val);
        }

        assertQuery("SELECT * FROM t")
                .returns((byte) 1, (short) 1, 1, 1L, BigDecimal.valueOf(1), 1f, 1d)
                .returns((byte) 2, (short) 2, 2, 2L, BigDecimal.valueOf(2), 2f, 2d)
                .returns((byte) 3, (short) 3, 3, 3L, BigDecimal.valueOf(3), 3f, 3d)
                .returns((byte) 4, (short) 4, 4, 4L, BigDecimal.valueOf(4), 4f, 4d)
                .returns((byte) 5, (short) 5, 5, 5L, BigDecimal.valueOf(5), 5f, 5d)
                .returns((byte) 6, (short) 6, 6, 6L, BigDecimal.valueOf(6), 6f, 6d)
                .returns((byte) 7, (short) 7, 7, 7L, BigDecimal.valueOf(7), 7f, 7d)
                .check();
    }

    /**
     * Test cases for decimal literals.
     */
    @Test
    public void testDecimalLiteral() {
        sql("CREATE TABLE tbl(id int PRIMARY KEY, val DECIMAL(32, 5))");

        assertQuery("SELECT DECIMAL '-123.0'").returns(new BigDecimal(("-123.0"))).check();
        assertQuery("SELECT DECIMAL '10'").returns(new BigDecimal(("10"))).check();
        assertQuery("SELECT DECIMAL '10.000'").returns(new BigDecimal(("10.000"))).check();

        assertQuery("SELECT DECIMAL '10.000' + DECIMAL '0.1'").returns(new BigDecimal(("10.100"))).check();
        assertQuery("SELECT DECIMAL '10.000' - DECIMAL '0.01'").returns(new BigDecimal(("9.990"))).check();
        assertQuery("SELECT DECIMAL '10.000' * DECIMAL '0.01'").returns(new BigDecimal(("0.10000"))).check();
        assertQuery("SELECT DECIMAL '10.000' / DECIMAL '0.01'").returns(new BigDecimal(("1000.0"))).check();

        assertQuery("SELECT DECIMAL '10.000' = '10.000'").returns(true).check();
        assertQuery("SELECT DECIMAL '10.000' = '10.001'").returns(false).check();

        assertQuery("SELECT CASE WHEN true THEN DECIMAL '1.00' ELSE DECIMAL '0' END")
                .returns(new BigDecimal("1.00")).check();

        assertQuery("SELECT CASE WHEN false THEN DECIMAL '1.00' ELSE DECIMAL '0.0' END")
                .returns(new BigDecimal("0.0")).check();

        assertQuery("SELECT DECIMAL \"10\" FROM (SELECT 1 as decimal) tmp").returns(1).check();

        assertQuery(
                "SELECT DECIMAL '0.09'  BETWEEN DECIMAL '0.06' AND DECIMAL '0.07'")
                .returns(false).check();

        assertQuery("SELECT ROUND(DECIMAL '10.000', 2)").returns(new BigDecimal("10.00")).check();
        assertQuery("SELECT CAST(DECIMAL '10.000' AS VARCHAR)").returns("10.000").check();
        assertQuery("SELECT CAST(DECIMAL '10.000' AS INTEGER)").returns(10).check();

        sql("INSERT INTO tbl VALUES(1, DECIMAL '10.01')");

        assertQuery("SELECT val FROM tbl").returns(new BigDecimal("10.01000")).check();

        assertQuery("SELECT id FROM tlb WHERE val = DECIMAL '10.0'").returns(1);

        sql("UPDATE tbl SET val=DECIMAL '10.20' WHERE id = 1");
        assertQuery("SELECT id FROM tbl WHERE val = DECIMAL '10.20'").returns(1);
    }

    private LocalDate sqlDate(String str) {
        return LocalDate.parse(str);
    }

    private LocalDateTime sqlDateTime(String str) {
        return LocalDateTime.parse(str);
    }
}
