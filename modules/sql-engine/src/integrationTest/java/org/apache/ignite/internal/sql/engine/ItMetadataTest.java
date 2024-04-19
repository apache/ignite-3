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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.generate;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.MAX_LENGTH_OF_ALIASES;
import static org.apache.ignite.sql.ColumnMetadata.UNDEFINED_SCALE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Group of tests to verify the query metadata returned alongside the query result.
 */
public class ItMetadataTest extends BaseSqlIntegrationTest {
    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        createAndPopulateTable();
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    public void trimColumnNames() {
        String var300 = "'" + generate(() -> "X").limit(300).collect(joining()) + "'";
        String var256 = var300.substring(0, Math.min(var300.length(), MAX_LENGTH_OF_ALIASES));

        assertQuery("select " + var300 + " from person").columnNames(var256).check();
    }

    @Test
    public void columnNames() {
        assertQuery("select (select count(*) from person), (select avg(salary) from person) from person")
                .columnNames("EXPR$0", "EXPR$1").check();
        assertQuery("select (select count(*) from person) as subquery from person")
                .columnNames("SUBQUERY").check();

        assertQuery("select salary*2, salary*2 as \"SaLaRy\", salary/2, salary+2, salary-2, mod(salary, 2)  from person")
                .columnNames("SALARY * 2", "SaLaRy", "SALARY / 2", "SALARY + 2", "SALARY - 2", "MOD(SALARY, 2)").check();
        assertQuery("select salary*2 as first, salary/2 as LAst from person").columnNames("FIRST", "LAST").check();

        assertQuery("select trim(name) tr_name from person").columnNames("TR_NAME").check();
        assertQuery("select trim(name) from person").columnNames("TRIM(BOTH ' ' FROM NAME)").check();
        assertQuery("select ceil(salary), floor(salary), position('text' IN salary) from person")
                .columnNames("CEIL(SALARY)", "FLOOR(SALARY)", "POSITION('text' IN SALARY)").check();

        assertQuery("select count(*) from person").columnNames("COUNT(*)").check();
        assertQuery("select count(name) from person").columnNames("COUNT(NAME)").check();
        assertQuery("select max(salary) from person").columnNames("MAX(SALARY)").check();
        assertQuery("select min(salary) from person").columnNames("MIN(SALARY)").check();
        assertQuery("select aVg(salary) from person").columnNames("AVG(SALARY)").check();
        assertQuery("select sum(salary) from person").columnNames("SUM(SALARY)").check();

        assertQuery("select typeOf(salary) from person").columnNames("TYPEOF(SALARY)").check();
        assertQuery("select typeOf(null) from person").columnNames("TYPEOF(NULL)").check();

        assertQuery("select salary, count(name) from person group by salary").columnNames("SALARY", "COUNT(NAME)").check();

        assertQuery("select 1, -1, 'some string' from person").columnNames("1", "-1", "'some string'").check();
    }

    @Test
    public void renameColumnsInFrom(){
        assertQuery("select NEW_PERSON.NEW_ID, NEW_NAME, NEW_persON.New_salary from person NEW_PERSON(NeW_Id, NeW_NaMe, New_SaLaRy)")
                .columnNames("NEW_ID", "NEW_NAME", "NEW_SALARY").check();
    }

    @Test
    public void infixTypeCast() {
        assertQuery("select id, id::tinyint as tid, id::smallint as sid, id::varchar as vid, id::interval hour, "
                + "id::interval year from person")
                .columnMetadata(
                        new MetadataMatcher().name("ID").type(ColumnType.INT32),
                        new MetadataMatcher().name("TID").type(ColumnType.INT8),
                        new MetadataMatcher().name("SID").type(ColumnType.INT16),
                        new MetadataMatcher().name("VID").type(ColumnType.STRING),
                        new MetadataMatcher().name("ID :: INTERVAL INTERVAL_HOUR").type(ColumnType.DURATION),
                        new MetadataMatcher().name("ID :: INTERVAL INTERVAL_YEAR").type(ColumnType.PERIOD)
                ).check();
    }

    @Test
    public void columnOrder() {
        sql("CREATE TABLE column_order (double_c DOUBLE, long_c BIGINT PRIMARY KEY, string_c VARCHAR, int_c INT)");
        sql("CREATE TABLE column_order1 (double_c DOUBLE, long_c BIGINT PRIMARY KEY, string_c VARCHAR)");

        assertQuery("select *, double_c, double_c * 2 from column_order")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C", "DOUBLE_C", "DOUBLE_C * 2")
                .check();

        assertQuery("select *, double_c as J, double_c * 2, double_c as J2 from column_order")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C", "J", "DOUBLE_C * 2", "J2")
                .check();

        assertQuery("select double_c * 2, * from column_order")
                .columnNames("DOUBLE_C * 2", "DOUBLE_C", "LONG_C", "STRING_C", "INT_C")
                .check();

        assertQuery("select *, *, double_c * 2 from column_order")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C", "DOUBLE_C0", "LONG_C0", "STRING_C0", "INT_C0", "DOUBLE_C * 2")
                .check();

        assertQuery("select *, double_c * 2, double_c * 2, * from column_order")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C", "DOUBLE_C * 2", "DOUBLE_C * 2", "DOUBLE_C0", "LONG_C0",
                        "STRING_C0", "INT_C0").check();

        assertQuery("select * from column_order")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C")
                .check();

        assertQuery("select a.*, a.double_c * 2, b.*  from column_order a, column_order1 b where a.double_c = b.double_c")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C", "A.DOUBLE_C * 2", "DOUBLE_C0", "LONG_C0", "STRING_C0")
                .check();

        assertQuery("select a.*, a.double_c * 2, a.double_c * 2 as J, b.*  from column_order a, column_order1 b "
                + "where a.double_c = b.double_c")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C", "A.DOUBLE_C * 2", "J", "DOUBLE_C0", "LONG_C0", "STRING_C0")
                .check();
    }

    @Test
    public void metadata() {
        sql("CREATE TABLE METADATA_TABLE (" + "ID INT PRIMARY KEY, "
                + "BOOLEAN_C BOOLEAN, "

                // Exact numeric types
                + "TINY_C TINYINT, " // TINYINT is not a part of any SQL standard.
                + "SMALL_C SMALLINT, " + "INT_C INT, " + "LONG_C BIGINT, " + "NUMBER_C NUMERIC, " + "NUMBER_C2 NUMERIC(38), "
                + "NUMBER_C3 NUMERIC(38,37), " + "DECIMAL_C DECIMAL, " + "DECIMAL_C2 DECIMAL(38), " + "DECIMAL_C3 DECIMAL(38,37), "

                // Approximate numeric types
                + "FLOAT_C FLOAT, " // FLOAT(4) ANSI`92 syntax is not supported by Calcite parser.
                + "REAL_C REAL, " + "DOUBLE_C DOUBLE, "

                // Character string types
                + "CHAR_C CHAR, " + "CHAR_C2 CHAR(65536), " + "VARCHAR_C VARCHAR, " + "VARCHAR_C2 VARCHAR(125), "

                // Binary string types
                + "BINARY_C BINARY, " + "BINARY_C2 BINARY(65536), " + "VARBINARY_C VARBINARY, " + "VARBINARY_C2 VARBINARY(125), "

                // Datetime types
                // ANSI`99 syntax "WITH TIME ZONE" is not supported,
                // a "WITH LOCAL TIME ZONE" syntax MUST be used instead.
                + "DATE_C DATE, " + "TIME_C TIME, " + "TIME_C2 TIME(9), "
                // TODO: IGNITE-21555 Ignite doesn't support TIME_WITH_LOCAL_TIME_ZONE data type.
                // + "TIME_LTZ_C TIME WITH LOCAL TIME ZONE, "
                // + "TIME_LTZ_C2 TIME(9) WITH LOCAL TIME ZONE, "
                + "DATETIME_C TIMESTAMP, " + "DATETIME_C2 TIMESTAMP(9), "
                 + "TIMESTAMP_C TIMESTAMP WITH LOCAL TIME ZONE, "
                 + "TIMESTAMP_C2 TIMESTAMP(9) WITH LOCAL TIME ZONE, "

                // Interval types
                // TODO: IGNITE-17373: Ignite doesn't support interval types yet.
                // + "INTERVAL_YEAR_C INTERVAL YEAR, "
                // + "INTERVAL_MONTH_C INTERVAL MONTH, "
                // + "INTERVAL_DAY_C INTERVAL DAY, "
                // + "INTERVAL_HOUR_C INTERVAL HOUR, "
                // + "INTERVAL_SEC_C INTERVAL SECOND, "
                // + "INTERVAL_SEC_C2 INTERVAL SECOND(9), "

                // Custom types
                + "UUID_C UUID, "
                // TODO: IGNITE-18431: Sql. BitSet is not supported.
                // + "BITSET_C BITMASK, "
                // + "BITSET_C BITMASK(8), "

                // Nullability constraint
                + "NULLABLE_C INT, " + "NON_NULL_C INT NOT NULL " + ")");

        assertQuery("select * from metadata_table")
                .columnMetadata(
                        new MetadataMatcher().name("ID").nullable(false),
                        new MetadataMatcher().name("BOOLEAN_C"),

                        // Exact numeric types
                        new MetadataMatcher().name("TINY_C").type(ColumnType.INT8).precision(3).scale(0),
                        new MetadataMatcher().name("SMALL_C").type(ColumnType.INT16).precision(5).scale(0),
                        new MetadataMatcher().name("INT_C").type(ColumnType.INT32).precision(10).scale(0),
                        new MetadataMatcher().name("LONG_C").type(ColumnType.INT64).precision(19).scale(0),

                        new MetadataMatcher().name("NUMBER_C").type(ColumnType.DECIMAL).precision(0x7FFF).scale(0),
                        new MetadataMatcher().name("NUMBER_C2").type(ColumnType.DECIMAL).precision(38).scale(0),
                        new MetadataMatcher().name("NUMBER_C3").type(ColumnType.DECIMAL).precision(38).scale(37),
                        new MetadataMatcher().name("DECIMAL_C").type(ColumnType.DECIMAL).precision(0x7FFF).scale(0),
                        new MetadataMatcher().name("DECIMAL_C2").type(ColumnType.DECIMAL).precision(38).scale(0),
                        new MetadataMatcher().name("DECIMAL_C3").type(ColumnType.DECIMAL).precision(38).scale(37),

                        // Approximate numeric types
                        new MetadataMatcher().name("FLOAT_C").type(ColumnType.FLOAT).precision(7).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("REAL_C").type(ColumnType.FLOAT).precision(7).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("DOUBLE_C").type(ColumnType.DOUBLE).precision(15).scale(UNDEFINED_SCALE),

                        // Character string types
                        new MetadataMatcher().name("CHAR_C").type(ColumnType.STRING).precision(1).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("CHAR_C2").type(ColumnType.STRING).precision(65536).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("VARCHAR_C").type(ColumnType.STRING).precision(65536).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("VARCHAR_C2").type(ColumnType.STRING).precision(125).scale(UNDEFINED_SCALE),

                        // Binary string types
                        new MetadataMatcher().name("BINARY_C").type(ColumnType.BYTE_ARRAY).precision(1).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("BINARY_C2").type(ColumnType.BYTE_ARRAY).precision(65536).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("VARBINARY_C").type(ColumnType.BYTE_ARRAY).precision(65536).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("VARBINARY_C2").type(ColumnType.BYTE_ARRAY).precision(125).scale(UNDEFINED_SCALE),

                        // Datetime types
                        new MetadataMatcher().name("DATE_C").type(ColumnType.DATE).precision(0).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIME_C").type(ColumnType.TIME).precision(0).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIME_C2").type(ColumnType.TIME).precision(9).scale(UNDEFINED_SCALE),
                        // TODO: IGNITE-21555 Ignite doesn't support TIME_WITH_LOCAL_TIME_ZONE data type.
                        // new MetadataMatcher().name("TIME_LTZ_C").type(ColumnType.TIME).precision(0).scale(UNDEFINED_SCALE),
                        // new MetadataMatcher().name("TIME_LTZ_C2").type(ColumnType.TIME).precision(9).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("DATETIME_C").type(ColumnType.DATETIME).precision(6).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("DATETIME_C2").type(ColumnType.DATETIME).precision(9).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIMESTAMP_C").type(ColumnType.TIMESTAMP).precision(6).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIMESTAMP_C2").type(ColumnType.TIMESTAMP).precision(9).scale(UNDEFINED_SCALE),

                        // Interval types
                        // TODO: IGNITE-17373: Ignite doesn't support interval types yet.
                        // new MetadataMatcher().name("INTERVAL_YEAR_C"),
                        // new MetadataMatcher().name("INTERVAL_MONTH_C"),
                        // new MetadataMatcher().name("INTERVAL_DAY_C"),
                        // new MetadataMatcher().name("INTERVAL_HOUR_C"),
                        // new MetadataMatcher().name("INTERVAL_MINUTE_C"),
                        // new MetadataMatcher().name("INTERVAL_SEC_C"),
                        // new MetadataMatcher().name("INTERVAL_SEC_C2"),

                        // Custom types
                        new MetadataMatcher().name("UUID_C"),
                        // TODO: IGNITE-18431: Sql. BitSet is not supported.
                        // new MetadataMatcher().name("BITSET_C"),
                        // new MetadataMatcher().name("BITSET_C2"),

                        // Nullability constraint
                        new MetadataMatcher().name("NULLABLE_C").nullable(true),
                        new MetadataMatcher().name("NON_NULL_C").nullable(false)
                )
                .check();
    }

    @Test
    public void caseSensitivity() {
        sql("CREATE TABLE sens(\"Col1\" int, col2 int, \"Col3.a\" int, COL4 int, PRIMARY KEY(\"Col1\", col2))");

        assertQuery("select * from sens")
                .columnMetadata(
                        new MetadataMatcher().name("Col1"),
                        new MetadataMatcher().name("COL2"),
                        new MetadataMatcher().name("Col3.a"),
                        new MetadataMatcher().name("COL4"))
                .check();

        sql("INSERT INTO sens VALUES (1, 1, 1, 1)");

        ResultSet<SqlRow> res = igniteSql().execute(null, "select * from sens");
        SqlRow row = res.next();
        assertNotNull(row.intValue("\"Col1\""));
        assertThrows(IllegalArgumentException.class, () -> row.intValue("col1"));
        assertThrows(IllegalArgumentException.class, () -> row.intValue("Col1"));
        assertThrows(IllegalArgumentException.class, () -> row.intValue("COL1"));

        assertNotNull(row.intValue("col2"));
        assertNotNull(row.intValue("COL2"));

        assertThrows(IllegalArgumentException.class, () -> row.intValue("\""));
        assertThrows(IllegalArgumentException.class, () -> row.intValue(""));

        assertNotNull(row.intValue("\"Col3.a\""));
        assertThrows(IllegalArgumentException.class, () -> row.intValue("col3.a"));
        assertThrows(IllegalArgumentException.class, () -> row.intValue("Col3.a"));
        assertThrows(IllegalArgumentException.class, () -> row.intValue("COL3.a"));

        assertNotNull(row.intValue("col4"));
        assertNotNull(row.intValue("COL4"));
    }
}
