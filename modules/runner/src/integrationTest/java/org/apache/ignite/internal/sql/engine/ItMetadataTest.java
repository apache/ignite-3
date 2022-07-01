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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.generate;
import static org.apache.ignite.sql.ColumnMetadata.UNDEFINED_SCALE;

import java.time.Duration;
import java.time.Period;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.sql.SqlColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Group of tests to verify the query metadata returned alongside the query result.
 */
public class ItMetadataTest extends AbstractBasicIntegrationTest {
    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        createAndPopulateTable();
    }

    @Override
    protected int nodes() {
        return 1;
    }

    @Test
    public void trimColumnNames() {
        String var300 = generate(() -> "X").limit(300).collect(joining());
        String var256 = "'" + var300.substring(0, 255);

        assertQuery("select '" + var300 + "' from person").columnNames(var256).check();
    }

    @Test
    public void columnNames() {
        assertQuery("select (select count(*) from person), (select avg(salary) from person) from person")
                .columnNames("EXPR$0", "EXPR$1").check();
        assertQuery("select (select count(*) from person) as subquery from person")
                .columnNames("SUBQUERY").check();

        assertQuery("select salary*2, salary/2, salary+2, salary-2, mod(salary, 2)  from person")
                .columnNames("SALARY * 2", "SALARY / 2", "SALARY + 2", "SALARY - 2", "MOD(SALARY, 2)").check();
        assertQuery("select salary*2 as first, salary/2 as secOND from person").columnNames("FIRST", "SECOND").check();

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

        assertQuery("select salary, count(name) from person group by salary").columnNames("SALARY", "COUNT(NAME)").check();

        assertQuery("select 1, -1, 'some string' from person").columnNames("1", "-1", "'some string'").check();
    }

    @Test
    public void infixTypeCast() {
        assertQuery("select id, id::tinyint as tid, id::smallint as sid, id::varchar as vid, id::interval hour, "
                + "id::interval year from person")
                .columnNames("ID", "TID", "SID", "VID", "ID :: INTERVAL INTERVAL_HOUR", "ID :: INTERVAL INTERVAL_YEAR")
                .columnTypes(Integer.class, Byte.class, Short.class, String.class, Duration.class, Period.class)
                .check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16679")
    public void columnOrder() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "COLUMN_ORDER").columns(
                SchemaBuilders.column("DOUBLE_C", ColumnType.DOUBLE).asNullable(true).build(),
                SchemaBuilders.column("LONG_C", ColumnType.INT64).build(),
                SchemaBuilders.column("STRING_C", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("INT_C", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("LONG_C").build();

        CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        assertQuery("select * from column_order")
                .columnNames("DOUBLE_C", "LONG_C", "STRING_C", "INT_C")
                .check();
    }

    @Test
    public void metadata() {
        CLUSTER_NODES.get(0).sql().createSession()
                .execute(null, "CREATE TABLE METADATA_TABLE ("
                                + "ID INT PRIMARY KEY, "
//                                + "BOOLEAN_C BOOLEAN, "  //TODO: from ANSI`99. Not supported by Ignite.

                                // Exact numeric types
                                + "TINY_C TINYINT, " // TINYINT is not a part of any SQL standard.
                                + "SMALL_C SMALLINT, "
                                + "INT_C INT, "
                                + "LONG_C BIGINT, "
                                + "NUMBER_C NUMERIC, "
                                + "NUMBER_C2 NUMERIC(38), "
                                + "NUMBER_C3 NUMERIC(38,37), "
                                + "DECIMAL_C DECIMAL, "
                                + "DECIMAL_C2 DECIMAL(38), "
                                + "DECIMAL_C3 DECIMAL(38,37), "

                                // Approximate numeric types
                                + "FLOAT_C FLOAT, "
//                              + "FLOAT_C2 FLOAT(4), " // TODO: from ANSI`92. Not supported by Calcite parser.
                                + "REAL_C REAL, "
                                + "DOUBLE_C DOUBLE, "

                                // Character string types
                                + "CHAR_C CHAR, "
                                + "CHAR_C2 CHAR(65536), "
                                + "VARCHAR_C VARCHAR, "
                                + "VARCHAR_C2 VARCHAR(125), "

                                // Binary string types
//                              + "BIT_C BIT, " // TODO: from ANSI`92. Not supported by Calcite parser.
//                              + "BIT_C2 BIT(10), "  // TODO: from ANSI`92. Not supported by Calcite parser.
//                              + "BIT_C3 BIT VARYING(10), " // TODO: from ANSI`92. Not supported by Calcite parser.
                                + "BINARY_C BINARY, " // Added in ANSI`99
                                + "BINARY_C2 BINARY(65536), "
//                              + "VARBINARY_C VARBINARY, " // TODO: from ANSI`99. Not supported by Calcite parser.
//                              + "VARBINARY_C2 VARBINARY(125) " // TODO: from ANSI`99. Not supported by Calcite parser.

                                // Datetime types
                                + "DATE_C DATE, "
                                + "TIME_C TIME, "
                                + "TIME_C2 TIME(9), "
                                + "TIME_LTZ_C TIME WITH LOCAL TIME ZONE, " // Not part of any standard
                                + "TIME_LTZ_C2 TIME(9) WITH LOCAL TIME ZONE, " // Not part of any standard
//                              + "TIME_TZ_C TIME WITH TIMEZONE, " // TODO: from ANSI`92. Not supported by Calcite parser.
//                              + "TIME_TZ_C2 TIME(9) WITH TIMEZONE, " // TODO: from ANSI`92. Not supported by Calcite parser.
                                + "DATETIME_C TIMESTAMP, "
                                + "DATETIME_C2 TIMESTAMP(9), "
                                + "TIMESTAMP_C TIMESTAMP WITH LOCAL TIME ZONE, " // Not part of any standard
                                + "TIMESTAMP_C2 TIMESTAMP(9) WITH LOCAL TIME ZONE, " // Not part of any standard
//                              + "TIMESTAMP_C TIMESTAMP WITH TIME ZONE, " // TODO: from ANSI`92. Not supported by Calcite parser.
//                              + "TIMESTAMP_C2 TIMESTAMP(9) WITH TIME ZONE, " // TODO: from ANSI`92. Not supported by Calcite parser.

                                // Interval types
                                // TODO: Ignite doesn't support interval types.
//                              + "INTERVAL_YEAR_C INTERVAL YEAR, "
//                              + "INTERVAL_MONTH_C INTERVAL MONTH, "
//                              + "INTERVAL_DAY_C INTERVAL DAY, "
//                              + "INTERVAL_HOUR_C INTERVAL HOUR, "
//                              + "INTERVAL_SEC_C INTERVAL SECOND, "
//                              + "INTERVAL_SEC_C2 INTERVAL SECOND(9), "

                                // Custom types
//                              + "UUID_C UUID, " //TODO: custom types are not supported yet.
//                              + "BITSET_C BITMASK(8) " //TODO: custom types are not supported yet. Map to BIT(n) ?

                                // Nullability constraint
                                + "NULLABLE_C INT, "
                                + "NON_NULL_C INT NOT NULL "
                                + ")"
                );

        assertQuery("select * from metadata_table")
                .columnMetadata(
                        new MetadataMatcher().name("ID").nullable(false),
//                      new MetadataMatcher().name("BOOLEAN_C"),

                        // Exact numeric types
                        new MetadataMatcher().name("TINY_C").type(SqlColumnType.INT8).precision(3).scale(0),
                        new MetadataMatcher().name("SMALL_C").type(SqlColumnType.INT16).precision(5).scale(0),
                        new MetadataMatcher().name("INT_C").type(SqlColumnType.INT32).precision(10).scale(0),
                        new MetadataMatcher().name("LONG_C").type(SqlColumnType.INT64).precision(19).scale(0),

                        new MetadataMatcher().name("NUMBER_C").type(SqlColumnType.DECIMAL).precision(0x7FFF).scale(0),
                        new MetadataMatcher().name("NUMBER_C2").type(SqlColumnType.DECIMAL).precision(38).scale(0),
                        new MetadataMatcher().name("NUMBER_C3").type(SqlColumnType.DECIMAL).precision(38).scale(37),
                        new MetadataMatcher().name("DECIMAL_C").type(SqlColumnType.DECIMAL).precision(0x7FFF).scale(0),
                        new MetadataMatcher().name("DECIMAL_C2").type(SqlColumnType.DECIMAL).precision(38).scale(0),
                        new MetadataMatcher().name("DECIMAL_C3").type(SqlColumnType.DECIMAL).precision(38).scale(37),

                        // Approximate numeric types
                        new MetadataMatcher().name("FLOAT_C").type(SqlColumnType.FLOAT).precision(7).scale(UNDEFINED_SCALE),
//                      new MetadataMatcher().name("FLOAT_C2").precision(4).scale(ColumnMetadata.UNDEFINED_SCALE),
                        new MetadataMatcher().name("REAL_C").type(SqlColumnType.FLOAT).precision(7).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("DOUBLE_C").type(SqlColumnType.DOUBLE).precision(15).scale(UNDEFINED_SCALE),

                        // Character string types
                        new MetadataMatcher().name("CHAR_C").type(SqlColumnType.STRING).precision(1).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("CHAR_C2").type(SqlColumnType.STRING).precision(65536).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("VARCHAR_C").type(SqlColumnType.STRING).precision(65536).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("VARCHAR_C2").type(SqlColumnType.STRING).precision(125).scale(UNDEFINED_SCALE),

                        // Binary string types
//                      new MetadataMatcher().name("BIT_C"),
//                      new MetadataMatcher().name("BIT_C2"),
//                      new MetadataMatcher().name("BIT_C3"),
                        new MetadataMatcher().name("BINARY_C").type(SqlColumnType.BYTE_ARRAY).precision(1).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("BINARY_C2").type(SqlColumnType.BYTE_ARRAY).precision(65536).scale(UNDEFINED_SCALE),
//                      new MetadataMatcher().name("VARBINARY_C").type(SqlColumnType.BYTE_ARRAY).precision(65536).scale(UNDEFINED_SCALE),
//                      new MetadataMatcher().name("VARBINARY_C2").type(SqlColumnType.BYTE_ARRAY).precision(125).scale(UNDEFINED_SCALE),

                        // Datetime types
                        new MetadataMatcher().name("DATE_C").type(SqlColumnType.DATE).precision(0).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIME_C").type(SqlColumnType.TIME).precision(0).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIME_C2").type(SqlColumnType.TIME).precision(9).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIME_LTZ_C").type(SqlColumnType.TIME).precision(0).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIME_LTZ_C2").type(SqlColumnType.TIME).precision(9).scale(UNDEFINED_SCALE),
//                      new MetadataMatcher().name("TIME_TZ_C").type(SqlColumnType.TIME),
//                      new MetadataMatcher().name("TIME_TZ_C2").type(SqlColumnType.TIME),
                        new MetadataMatcher().name("DATETIME_C").type(SqlColumnType.DATETIME).precision(0).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("DATETIME_C2").type(SqlColumnType.DATETIME).precision(9).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIMESTAMP_C").type(SqlColumnType.TIMESTAMP).precision(6).scale(UNDEFINED_SCALE),
                        new MetadataMatcher().name("TIMESTAMP_C2").type(SqlColumnType.TIMESTAMP).precision(9).scale(UNDEFINED_SCALE),
//                      new MetadataMatcher().name("TIMESTAMP_TZ_C").type(SqlColumnType.TIMESTAMP),
//                      new MetadataMatcher().name("TIMESTAMP_TZ_C2").type(SqlColumnType.TIMESTAMP),

                        // Interval types
                        // TODO: Ignite doesn't support interval types.
//                      new MetadataMatcher().name("INTERVAL_YEAR_C"),
//                      new MetadataMatcher().name("INTERVAL_MONTH_C"),
//                      new MetadataMatcher().name("INTERVAL_DAY_C"),
//                      new MetadataMatcher().name("INTERVAL_HOUR_C"),
//                      new MetadataMatcher().name("INTERVAL_MINUTE_C"),
//                      new MetadataMatcher().name("INTERVAL_SEC_C"),
//                      new MetadataMatcher().name("INTERVAL_SEC_C2"),

                        // Custom types
//                      new MetadataMatcher().name("UUID_C"),
//                      new MetadataMatcher().name("BITSET_C"),

                        // Nullability constraint
                        new MetadataMatcher().name("NULLABLE_C").nullable(true),
                        new MetadataMatcher().name("NON_NULL_C").nullable(false)
                )
                .check();
    }
}
