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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test SQL data types.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-16679")
public class ItDataTypesTest extends AbstractBasicIntegrationTest {
    /** Tests correctness with unicode. */
    @Test
    public void testUnicodeStrings() {
        try {
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
        } finally {
            sql("DROP TABLE IF EXISTS string_table");
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
        assertThrows(IgniteException.class, () -> sql("INSERT INTO tbl(c1, c2) VALUES (2, NULL)"));
    }

    /**
     * Tests numeric types mapping on Java types.
     */
    @Test
    public void testNumericRanges() {
        try {
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
        } finally {
            sql("DROP TABLE IF EXISTS tbl");
        }
    }

    /**
     * Tests numeric type convertation on equals.
     */
    @Test
    public void testNumericConvertingOnEquals() {
        try {
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
        } finally {
            sql("DROP TABLE if exists tbl");
        }
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

    private LocalDate sqlDate(String str) {
        return LocalDate.parse(str);
    }

    private LocalDateTime sqlDateTime(String str) {
        return LocalDateTime.parse(str);
    }
}
