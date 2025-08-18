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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.ItTemporalIndexTest.Parser.dateTime;
import static org.apache.ignite.internal.sql.engine.ItTemporalIndexTest.Parser.instant;
import static org.apache.ignite.internal.sql.engine.ItTemporalIndexTest.Parser.time;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScanIgnoreBounds;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.matchesOnce;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.SQL_CONFORMANT_DATETIME_FORMATTER;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Integration tests check temporal indexes. */
public class ItTemporalIndexTest extends BaseSqlIntegrationTest {
    private static final LocalDate INITIAL_DATE = LocalDate.of(1552, 10, 1);
    private static final LocalTime INITIAL_TIME = LocalTime.of(0, 0, 0);
    private static final LocalDateTime INITIAL_TS = LocalDateTime.of(1552, 10, 1, 0, 0);
    private static final Instant INITIAL_TS_LTZ = INITIAL_TS.toInstant(ZoneOffset.UTC);

    private static final Map<String, String[]> All_INDEXES = Map.of(
            "DATE1", new String[]{"S_ASC_IDX_DATE1", "S_DESC_IDX_DATE1", "HASH_IDX_DATE1"},
            "TIME1", new String[]{"S_ASC_IDX_TIME1", "S_DESC_IDX_TIME1", "HASH_IDX_TIME1"},
            "TIMESTAMP1", new String[]{"S_ASC_IDX_TIMESTAMP1", "S_DESC_IDX_TIMESTAMP1", "HASH_IDX_TIMESTAMP1"},
            "TIMESTAMPTZ1", new String[]{"S_ASC_IDX_TZ1", "S_DESC_IDX_TZ1", "HASH_IDX_TZ1"}
    );

    private static final Map<String, String[]> SORTED_INDEXES = Map.of(
            "DATE1", new String[]{"S_ASC_IDX_DATE1", "S_DESC_IDX_DATE1", pkIndexName("DATE1")},
            "TIME1", new String[]{"S_ASC_IDX_TIME1", "S_DESC_IDX_TIME1", pkIndexName("TIME1")},
            "TIMESTAMP1", new String[]{"S_ASC_IDX_TIMESTAMP1", "S_DESC_IDX_TIMESTAMP1", pkIndexName("TIMESTAMP1")},
            "TIMESTAMPTZ1", new String[]{"S_ASC_IDX_TZ1", "S_DESC_IDX_TZ1", pkIndexName("TIMESTAMPTZ1")}
    );

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    static void initTestData() {
        String query = new IgniteStringBuilder()
                .app("CREATE TABLE DATE1 (pk DATE, val DATE, PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE DATE2 (pk DATE, val DATE, PRIMARY KEY USING HASH (pk));").nl()
                .app("CREATE TABLE TIME1 (pk TIME, val TIME, PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE TIME2 (pk TIME, val TIME, PRIMARY KEY USING HASH (pk));").nl()
                .app("CREATE TABLE TIMESTAMP1 (pk TIMESTAMP, val TIMESTAMP, PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE TIMESTAMP2 (pk TIMESTAMP, val TIMESTAMP, PRIMARY KEY USING HASH (pk));").nl()
                .app("CREATE TABLE TIMESTAMPTZ1 (pk TIMESTAMP WITH LOCAL TIME ZONE, val TIMESTAMP WITH LOCAL TIME ZONE, "
                        + "PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE TIMESTAMPTZ2 (pk TIMESTAMP WITH LOCAL TIME ZONE, val TIMESTAMP WITH LOCAL TIME ZONE, "
                        + "PRIMARY KEY USING HASH (pk));").nl()

                .app("CREATE INDEX s_asc_idx_date1 ON date1 USING SORTED (val ASC);")
                .app("CREATE INDEX s_desc_idx_date1 ON date1 USING SORTED (val DESC);")
                .app("CREATE INDEX hash_idx_date1 ON date1 USING HASH (val);")

                .app("CREATE INDEX s_asc_idx_time1 ON time1 USING SORTED (val ASC);")
                .app("CREATE INDEX s_desc_idx_time1 ON time1 USING SORTED (val DESC);")
                .app("CREATE INDEX hash_idx_time1 ON time1 USING HASH (val);")

                .app("CREATE INDEX s_asc_idx_timestamp1 ON timestamp1 USING SORTED (val ASC);")
                .app("CREATE INDEX s_desc_idx_timestamp1 ON timestamp1 USING SORTED (val DESC);")
                .app("CREATE INDEX hash_idx_timestamp1 ON timestamp1 USING HASH (val);")

                .app("CREATE INDEX s_asc_idx_tz1 ON timestamptz1 USING SORTED (val ASC);")
                .app("CREATE INDEX s_desc_idx_tz1 ON timestamptz1 USING SORTED (val DESC);")
                .app("CREATE INDEX hash_idx_tz1 ON timestamptz1 USING HASH (val);")

                .app("CREATE TABLE T_TIME(ID INT PRIMARY KEY,"
                        + "col_time_0 TIME(0), col_time_1 TIME(1), col_time_2 TIME(2), col_time_3 TIME(3), col_time_4 TIME(4));").nl()

                .app("CREATE INDEX T_TIME_IDX_SORTED_0 ON T_TIME USING SORTED(col_time_0);").nl()
                .app("CREATE INDEX T_TIME_IDX_SORTED_1 ON T_TIME USING SORTED(col_time_1);").nl()
                .app("CREATE INDEX T_TIME_IDX_SORTED_2 ON T_TIME USING SORTED(col_time_2);").nl()
                .app("CREATE INDEX T_TIME_IDX_SORTED_3 ON T_TIME USING SORTED(col_time_3);").nl()
                .app("CREATE INDEX T_TIME_IDX_SORTED_4 ON T_TIME USING SORTED(col_time_4);").nl()

                .app("CREATE INDEX T_TIME_IDX_HASH_0 ON T_TIME USING HASH(col_time_0);").nl()
                .app("CREATE INDEX T_TIME_IDX_HASH_1 ON T_TIME USING HASH(col_time_1);").nl()
                .app("CREATE INDEX T_TIME_IDX_HASH_2 ON T_TIME USING HASH(col_time_2);").nl()
                .app("CREATE INDEX T_TIME_IDX_HASH_3 ON T_TIME USING HASH(col_time_3);").nl()
                .app("CREATE INDEX T_TIME_IDX_HASH_4 ON T_TIME USING HASH(col_time_4);").nl()

                .app("CREATE TABLE T_TIMESTAMP(ID INT PRIMARY KEY,"
                        + "col_timestamp_0 TIMESTAMP(0), col_timestamp_1 TIMESTAMP(1), col_timestamp_2 TIMESTAMP(2), "
                        + "col_timestamp_3 TIMESTAMP(3), col_timestamp_4 TIMESTAMP(4));").nl()

                .app("CREATE INDEX T_TIMESTAMP_IDX_SORTED_0 ON T_TIMESTAMP USING SORTED(col_timestamp_0);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_SORTED_1 ON T_TIMESTAMP USING SORTED(col_timestamp_1);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_SORTED_2 ON T_TIMESTAMP USING SORTED(col_timestamp_2);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_SORTED_3 ON T_TIMESTAMP USING SORTED(col_timestamp_3);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_SORTED_4 ON T_TIMESTAMP USING SORTED(col_timestamp_4);").nl()

                .app("CREATE INDEX T_TIMESTAMP_IDX_HASH_0 ON T_TIMESTAMP USING HASH(col_timestamp_0);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_HASH_1 ON T_TIMESTAMP USING HASH(col_timestamp_1);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_HASH_2 ON T_TIMESTAMP USING HASH(col_timestamp_2);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_HASH_3 ON T_TIMESTAMP USING HASH(col_timestamp_3);").nl()
                .app("CREATE INDEX T_TIMESTAMP_IDX_HASH_4 ON T_TIMESTAMP USING HASH(col_timestamp_4);").nl()

                .app("CREATE TABLE T_TIMESTAMP_WITH_LOCAL_TIME_ZONE(ID INT PRIMARY KEY,"
                        + "col_timestamp_with_local_time_zone_0 TIMESTAMP(0) WITH LOCAL TIME ZONE,"
                        + "col_timestamp_with_local_time_zone_1 TIMESTAMP(1) WITH LOCAL TIME ZONE,"
                        + "col_timestamp_with_local_time_zone_2 TIMESTAMP(2) WITH LOCAL TIME ZONE,"
                        + "col_timestamp_with_local_time_zone_3 TIMESTAMP(3) WITH LOCAL TIME ZONE,"
                        + "col_timestamp_with_local_time_zone_4 TIMESTAMP(4) WITH LOCAL TIME ZONE);").nl()

                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_SORTED_0 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING SORTED(col_timestamp_with_local_time_zone_0);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_SORTED_1 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING SORTED(col_timestamp_with_local_time_zone_1);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_SORTED_2 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING SORTED(col_timestamp_with_local_time_zone_2);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_SORTED_3 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING SORTED(col_timestamp_with_local_time_zone_3);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_SORTED_4 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING SORTED(col_timestamp_with_local_time_zone_4);").nl()

                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_HASH_0 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING HASH(col_timestamp_with_local_time_zone_0);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_HASH_1 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING HASH(col_timestamp_with_local_time_zone_1);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_HASH_2 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING HASH(col_timestamp_with_local_time_zone_2);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_HASH_3 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING HASH(col_timestamp_with_local_time_zone_3);").nl()
                .app("CREATE INDEX T_TIMESTAMP_WITH_LOCAL_TIME_ZONE_IDX_HASH_4 ON T_TIMESTAMP_WITH_LOCAL_TIME_ZONE "
                        + "USING HASH(col_timestamp_with_local_time_zone_4);").nl()

                .toString();

        sqlScript(query);

        fillData("DATE1");
        fillData("DATE2");

        fillData("TIME1");
        fillData("TIME2");

        fillData("TIMESTAMP1");
        fillData("TIMESTAMP2");

        fillData("TIMESTAMPTZ1");
        fillData("TIMESTAMPTZ2");

        fillData("T_TIME");
        fillData("T_TIMESTAMP");
        fillData("T_TIMESTAMP_WITH_LOCAL_TIME_ZONE");
    }

    @ParameterizedTest(name = "table = {0}, predicate = {1}")
    @MethodSource("pkExactSearchArguments")
    public void testSearchExactPk(String table, String predicate, Object result) {
        assertQuery(format("SELECT val FROM {} WHERE pk = {}", table, predicate))
                .matches(matchesOnce("KeyValueGet"))
                .returns(result)
                .check();

        // Dynamic parameter.
        assertQuery(format("SELECT val FROM {} WHERE pk = ?", table))
                .withParam(result)
                .matches(matchesOnce("KeyValueGet"))
                .returns(result)
                .check();
    }

    /** Check exact predicates. */
    @ParameterizedTest(name = "table = {0}, predicate = {1}")
    @MethodSource("pkExactSearchArguments")
    public void testSearchExactWithPkIdxUsage(String table, String predicate, Object result) {
        assertQuery(format("SELECT /*+ FORCE_INDEX({}), DISABLE_RULE('TableScanToKeyValueGetRule') */ val FROM {} WHERE pk = {}",
                pkIndexName(table), table, predicate))
                .matches(containsIndexScan("PUBLIC", table, pkIndexName(table)))
                .returns(result)
                .check();

        // Dynamic parameter.
        assertQuery(format("SELECT /*+ FORCE_INDEX({}), DISABLE_RULE('TableScanToKeyValueGetRule') */ val FROM {} WHERE pk = ?",
                pkIndexName(table), table))
                .withParam(result)
                .matches(containsIndexScan("PUBLIC", table, pkIndexName(table)))
                .returns(result)
                .check();

        if (All_INDEXES.containsKey(table)) {
            for (String idx : All_INDEXES.get(table)) {
                assertQuery(format("SELECT /*+ FORCE_INDEX({}) */ pk FROM {} WHERE val = {}",
                        idx, table, predicate))
                        .matches(containsIndexScan("PUBLIC", table, idx))
                        .returns(result)
                        .check();

                // Dynamic parameter.
                assertQuery(format("SELECT /*+ FORCE_INDEX({}) */ pk FROM {} WHERE val = ?",
                        idx, table))
                        .withParam(result)
                        .matches(containsIndexScan("PUBLIC", table, idx))
                        .returns(result)
                        .check();
            }
        }
    }

    /** Check range predicates. */
    @ParameterizedTest(name = "table = {0}, predicate = {1}")
    @MethodSource("geLeSearchArguments")
    public void testSearchGtLtWithPkIdxUsage(String table, String predicate, Object result) {
        String pkIndexName = pkIndexName(table);

        for (String idx : SORTED_INDEXES.get(table)) {
            assertQuery(format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk {} ORDER BY val",
                    idx, table, predicate))
                    .matches(pkIndexName.equals(idx)
                            ? containsIndexScan("PUBLIC", table, idx)
                            : containsIndexScanIgnoreBounds("PUBLIC", table, idx))
                    .returns(result)
                    .check();
        }
    }

    /** Check range predicates with dynamic parameter. */
    @ParameterizedTest(name = "table = {0}, predicate = {1}")
    @MethodSource("geLeSearchDynParamArguments")
    public void testSearchGtLtWithPkIdxUsageDynamicParam(String table, String predicate, Object parameter, Object result) {
        String pkIndexName = pkIndexName(table);

        for (String idx : SORTED_INDEXES.get(table)) {
            assertQuery(format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk {} ORDER BY val",
                    idx, table, predicate))
                    .withParam(parameter)
                    .matches(pkIndexName.equals(idx)
                            ? containsIndexScan("PUBLIC", table, idx)
                            : containsIndexScanIgnoreBounds("PUBLIC", table, idx))
                    .returns(result)
                    .check();
        }
    }

    /** Check range predicates. */
    @ParameterizedTest(name = "table = {0}, predicate = {1}")
    @MethodSource("betweenSearchArguments")
    public void testBetweenPkIdxUsage(String table, String predicate, Object[] result) {
        String pkIndexName = pkIndexName(table);

        for (String idx : SORTED_INDEXES.get(table)) {
            QueryChecker checker = assertQuery(
                    format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk {}", idx, table, predicate))
                    .matches(pkIndexName.equals(idx)
                            ? containsIndexScan("PUBLIC", table, idx)
                            : containsIndexScanIgnoreBounds("PUBLIC", table, idx));

            for (Object res : result) {
                checker.returns(res);
            }

            checker.check();
        }
    }

    /** Check range predicates with dynamic parameter. */
    @ParameterizedTest(name = "table = {0}")
    @MethodSource("betweenSearchDynParamArguments")
    public void testBetweenPkIdxUsageDynamicParam(String table, Object[] params, Object[] result) {
        String pkIndexName = pkIndexName(table);

        for (String idx : SORTED_INDEXES.get(table)) {
            QueryChecker checker = assertQuery(
                    format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk BETWEEN ? AND ?", idx, table))
                    .withParams(params)
                    .matches(pkIndexName.equals(idx)
                            ? containsIndexScan("PUBLIC", table, idx)
                            : containsIndexScanIgnoreBounds("PUBLIC", table, idx));

            for (Object res : result) {
                checker.returns(res);
            }

            checker.check();
        }
    }

    private static Stream<Arguments> betweenSearchArguments() {
        return Stream.of(
                Arguments.of("DATE1", " BETWEEN DATE '"
                        + INITIAL_DATE.plusDays(1)
                        + "' AND DATE '"
                        + INITIAL_DATE.plusDays(2)
                        + "'",
                        new Object[] {
                                INITIAL_DATE.plusDays(1),
                                INITIAL_DATE.plusDays(2)
                        }),

                Arguments.of("TIME1", " BETWEEN TIME '"
                        + INITIAL_TIME.plusSeconds(1)
                        + "' AND TIME '"
                        + INITIAL_TIME.plusSeconds(2)
                        + "'",
                        new Object[] {
                                INITIAL_TIME.plusSeconds(1),
                                INITIAL_TIME.plusSeconds(2)
                        }),

                Arguments.of("TIMESTAMP1", " BETWEEN TIMESTAMP '"
                        + INITIAL_TS.plusSeconds(1).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "' AND TIMESTAMP '"
                        + INITIAL_TS.plusSeconds(2).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "'",
                        new Object[] {
                                INITIAL_TS.plusSeconds(1),
                                INITIAL_TS.plusSeconds(2)
                        }),

                Arguments.of("TIMESTAMPTZ1", " BETWEEN TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.plusSeconds(1).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "' AND TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.plusSeconds(2).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "'",
                        new Object[] {
                                INITIAL_TS_LTZ.plusSeconds(1),
                                INITIAL_TS_LTZ.plusSeconds(2)
                        })
        );
    }

    private static Stream<Arguments> betweenSearchDynParamArguments() {
        return Stream.of(
                Arguments.of("DATE1",
                        new Object[]{
                                INITIAL_DATE.plusDays(1),
                                INITIAL_DATE.plusDays(2)
                        },
                        new Object[] {
                                INITIAL_DATE.plusDays(1),
                                INITIAL_DATE.plusDays(2)
                        }),

                Arguments.of("TIME1",
                        new Object[] {
                                INITIAL_TIME.plusSeconds(1),
                                INITIAL_TIME.plusSeconds(2)
                        },
                        new Object[] {
                                INITIAL_TIME.plusSeconds(1),
                                INITIAL_TIME.plusSeconds(2)
                        }),

                Arguments.of("TIMESTAMP1",
                        new Object[] {
                                INITIAL_TS.plusSeconds(1),
                                INITIAL_TS.plusSeconds(2)
                        },
                        new Object[] {
                                INITIAL_TS.plusSeconds(1),
                                INITIAL_TS.plusSeconds(2)
                        }),

                Arguments.of("TIMESTAMPTZ1",
                        new Object[] {
                                INITIAL_TS_LTZ.plusSeconds(1),
                                INITIAL_TS_LTZ.plusSeconds(2)
                        },
                        new Object[] {
                                INITIAL_TS_LTZ.plusSeconds(1),
                                INITIAL_TS_LTZ.plusSeconds(2)
                        })
        );
    }

    private static Stream<Arguments> geLeSearchArguments() {
        return Stream.of(
                Arguments.of("DATE1", " > DATE '" + INITIAL_DATE.plusDays(8) + "'", INITIAL_DATE.plusDays(9)),
                Arguments.of("DATE1", " >= DATE '" + INITIAL_DATE.plusDays(9) + "'", INITIAL_DATE.plusDays(9)),
                Arguments.of("DATE1", " < DATE '" + INITIAL_DATE.plusDays(1) + "'", INITIAL_DATE),
                Arguments.of("DATE1", " <= DATE '" + INITIAL_DATE + "'", INITIAL_DATE),

                Arguments.of("TIME1", " > TIME '" + INITIAL_TIME.plusSeconds(8) + "'", INITIAL_TIME.plusSeconds(9)),
                Arguments.of("TIME1", " >= TIME '" + INITIAL_TIME.plusSeconds(9) + "'", INITIAL_TIME.plusSeconds(9)),
                Arguments.of("TIME1", " < TIME '" + INITIAL_TIME.plusSeconds(1) + "'", INITIAL_TIME),
                Arguments.of("TIME1", " <= TIME '" + INITIAL_TIME.format(ISO_LOCAL_TIME) + "'", INITIAL_TIME),

                Arguments.of("TIMESTAMP1", " > TIMESTAMP '" + INITIAL_TS.plusSeconds(8).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "'", INITIAL_TS.plusSeconds(9)),
                Arguments.of("TIMESTAMP1", " >= TIMESTAMP '" + INITIAL_TS.plusSeconds(9).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "'", INITIAL_TS.plusSeconds(9)),

                Arguments.of("TIMESTAMP1", " < TIMESTAMP '" + INITIAL_TS.plusSeconds(1).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "'", INITIAL_TS),
                Arguments.of("TIMESTAMP1", " <= TIMESTAMP '" + INITIAL_TS.format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'", INITIAL_TS),

                Arguments.of("TIMESTAMPTZ1", " > TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.plusSeconds(8).format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'", INITIAL_TS_LTZ.plusSeconds(9)),
                Arguments.of("TIMESTAMPTZ1", " >= TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.plusSeconds(9).format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'", INITIAL_TS_LTZ.plusSeconds(9)),
                Arguments.of("TIMESTAMPTZ1", " < TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.plusSeconds(1).format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'", INITIAL_TS_LTZ),
                Arguments.of("TIMESTAMPTZ1", " <= TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'", INITIAL_TS_LTZ)
        );
    }

    private static Stream<Arguments> geLeSearchDynParamArguments() {
        return Stream.of(
                Arguments.of("DATE1", " > ?", INITIAL_DATE.plusDays(8), INITIAL_DATE.plusDays(9)),
                Arguments.of("DATE1", " >= ?", INITIAL_DATE.plusDays(9), INITIAL_DATE.plusDays(9)),
                Arguments.of("DATE1", " < ?", INITIAL_DATE.plusDays(1), INITIAL_DATE),
                Arguments.of("DATE1", " <= ?", INITIAL_DATE, INITIAL_DATE),

                Arguments.of("TIME1", " > ?", INITIAL_TIME.plusSeconds(8), INITIAL_TIME.plusSeconds(9)),
                Arguments.of("TIME1", " >= ?", INITIAL_TIME.plusSeconds(9), INITIAL_TIME.plusSeconds(9)),
                Arguments.of("TIME1", " < ?", INITIAL_TIME.plusSeconds(1), INITIAL_TIME),
                Arguments.of("TIME1", " <= ?", INITIAL_TIME, INITIAL_TIME),

                Arguments.of("TIMESTAMP1", " > ?", INITIAL_TS.plusSeconds(8), INITIAL_TS.plusSeconds(9)),
                Arguments.of("TIMESTAMP1", " >= ?", INITIAL_TS.plusSeconds(9), INITIAL_TS.plusSeconds(9)),

                Arguments.of("TIMESTAMP1", " < ?", INITIAL_TS.plusSeconds(1), INITIAL_TS),
                Arguments.of("TIMESTAMP1", " <= ?", INITIAL_TS, INITIAL_TS),

                Arguments.of("TIMESTAMPTZ1", " > ?", INITIAL_TS_LTZ.plusSeconds(8), INITIAL_TS_LTZ.plusSeconds(9)),
                Arguments.of("TIMESTAMPTZ1", " >= ?", INITIAL_TS_LTZ.plusSeconds(9), INITIAL_TS_LTZ.plusSeconds(9)),
                Arguments.of("TIMESTAMPTZ1", " < ?", INITIAL_TS_LTZ.plusSeconds(1), INITIAL_TS_LTZ),
                Arguments.of("TIMESTAMPTZ1", " <= ?", INITIAL_TS_LTZ, INITIAL_TS_LTZ)
        );
    }

    private static Stream<Arguments> pkExactSearchArguments() {
        return Stream.of(
                Arguments.of("DATE1", "DATE '" + INITIAL_DATE.plusDays(5) + "'", INITIAL_DATE.plusDays(5)),
                Arguments.of("DATE2", "DATE '" + INITIAL_DATE.plusDays(5) + "'", INITIAL_DATE.plusDays(5)),

                Arguments.of("TIME1", "TIME '" + INITIAL_TIME.plusSeconds(5) + "'", INITIAL_TIME.plusSeconds(5)),
                Arguments.of("TIME2", "TIME '" + INITIAL_TIME.plusSeconds(5) + "'", INITIAL_TIME.plusSeconds(5)),

                Arguments.of("TIMESTAMP1", "TIMESTAMP '" + INITIAL_TS.plusSeconds(5).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "'", INITIAL_TS.plusSeconds(5)),
                Arguments.of("TIMESTAMP2", "TIMESTAMP '" + INITIAL_TS.plusSeconds(5).format(SQL_CONFORMANT_DATETIME_FORMATTER)
                        + "'", INITIAL_TS.plusSeconds(5)),

                Arguments.of("TIMESTAMPTZ1", "TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.plusSeconds(5).format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'", INITIAL_TS_LTZ.plusSeconds(5)),
                Arguments.of("TIMESTAMPTZ2", "TIMESTAMP WITH LOCAL TIME ZONE '"
                        + INITIAL_TS.plusSeconds(5).format(SQL_CONFORMANT_DATETIME_FORMATTER) + "'", INITIAL_TS_LTZ.plusSeconds(5))
        );
    }

    @ParameterizedTest
    @MethodSource("temporalPrecisionLookupDynParamArgs")
    void testTemporalPrecisionLookupDynamicParam(SqlTypeName type, String condition, Temporal param, int precision,
            Temporal[] expectedRows) {
        makeCheckerForIndexWithPrecision(type, "sorted", condition, param, precision, expectedRows)
                .check();

        makeCheckerForIndexWithPrecision(type, "hash", condition, param, precision, expectedRows)
                .check();
    }

    @ParameterizedTest
    @MethodSource("temporalPrecisionLookupLiteralArgs")
    void testTemporalPrecisionLookupLiteral(SqlTypeName type, String condition, int precision, Temporal[] expectedRows) {
        makeCheckerForIndexWithPrecision(type, "sorted", condition, null, precision, expectedRows)
                .check();

        makeCheckerForIndexWithPrecision(type, "hash", condition, null, precision, expectedRows)
                .check();
    }

    private static List<Arguments> temporalPrecisionLookupDynParamArgs() {
        return temporalPrecisionLookupArgs().buildDynamicParamsArgs();
    }

    private static List<Arguments> temporalPrecisionLookupLiteralArgs() {
        return temporalPrecisionLookupArgs().buildLiteralArgs();
    }

    private static TemporalPrecisionTestArgsBuilder temporalPrecisionLookupArgs() {
        TemporalPrecisionTestArgsBuilder argsBuilder = new TemporalPrecisionTestArgsBuilder();

        argsBuilder.type(TIME)
                // The table is filled in as follows:
                //-------------------------------------------------------------------
                // TIME(0)  | TIME (1)   | TIME (2)    | TIME (3)     | TIME (4)
                //-------------------------------------------------------------------
                // 00:00:00 | 00:00:00   | 00:00:00    | 00:00:00     | 00:00:00
                // 00:00:00 | 00:00:00.1 | 00:00:00.1  | 00:00:00.1   | 00:00:00.1
                // 00:00:00 | 00:00:00.1 | 00:00:00.12 | 00:00:00.12  | 00:00:00.12
                // 00:00:00 | 00:00:00.1 | 00:00:00.12 | 00:00:00.123 | 00:00:00.123
                // 00:00:00 | 00:00:00.1 | 00:00:00.12 | 00:00:00.123 | 00:00:00.123
                .condition(" = {}")
                .param("00:00:00")
                // For the TIME(0) column we expect 5 rows with the same value.
                .results(0, fillArray(time("00:00:00"), 5))
                // For the TIME(1) column we expect 1 row with the value '00:00:00'.
                .results(1, time("00:00:00"))
                .results(2, time("00:00:00"))
                .results(3, time("00:00:00"))
                .results(4, time("00:00:00"))

                .param("00:00:00.1")
                .results(0)
                .results(1, fillArray(time("00:00:00.1"), 4))
                .results(2, time("00:00:00.1"))
                .results(3, time("00:00:00.1"))
                .results(4, time("00:00:00.1"))

                .param("00:00:00.12")
                .results(0)
                .results(1)
                .results(2, fillArray(time("00:00:00.12"), 3))
                .results(3, time("00:00:00.12"))
                .results(4, time("00:00:00.12"))

                .param("00:00:00.123")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(time("00:00:00.123"), 2))
                .results(4, fillArray(time("00:00:00.123"), 2))

                .param("00:00:00.1234")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(time("00:00:00.123"), 2))
                // TODO https://issues.apache.org/jira/browse/IGNITE-19162 00:00:00.1234 is expected
                .results(4, fillArray(time("00:00:00.123"), 2))

                // Downcast
                .param("00:00:00.123")
                .condition(" = {}::TIME(2)")
                .results(0)
                .results(1)
                .results(2, fillArray(time("00:00:00.12"), 3))
                .results(3, time("00:00:00.12"))
                .results(4, time("00:00:00.12"))

                .condition(" = {}::TIME(1)")
                .results(0)
                .results(1, fillArray(time("00:00:00.1"), 4))
                .results(2, time("00:00:00.1"))
                .results(3, time("00:00:00.1"))
                .results(4, time("00:00:00.1"))

                .condition(" = {}::TIME(0)")
                .results(0, fillArray(time("00:00:00"), 5))
                .results(1, time("00:00:00"))
                .results(2, time("00:00:00"))
                .results(3, time("00:00:00"))
                .results(4, time("00:00:00"))

                // Upcast
                .param("00:00:00.1")
                .condition(" = {}::TIME(2)")
                .results(0)
                .results(1, fillArray(time("00:00:00.1"), 4))
                .results(2, time("00:00:00.1"))
                .results(3, time("00:00:00.1"))
                .results(4, time("00:00:00.1"))

                .condition(" = {}::TIME(3)")
                .results(0)
                .results(1, fillArray(time("00:00:00.1"), 4))
                .results(2, time("00:00:00.1"))
                .results(3, time("00:00:00.1"))
                .results(4, time("00:00:00.1"))
                ;

        // TIMESTAMP
        argsBuilder.type(TIMESTAMP)
                .condition(" = {}")
                .param("1970-01-01 00:00:00")
                .results(0, fillArray(dateTime("1970-01-01 00:00:00"), 5))
                .results(1, dateTime("1970-01-01 00:00:00"))
                .results(2, dateTime("1970-01-01 00:00:00"))
                .results(3, dateTime("1970-01-01 00:00:00"))
                .results(4, dateTime("1970-01-01 00:00:00"))

                .param("1970-01-01 00:00:00.1")
                .results(0)
                .results(1, fillArray(dateTime("1970-01-01 00:00:00.1"), 4))
                .results(2, dateTime("1970-01-01 00:00:00.1"))
                .results(3, dateTime("1970-01-01 00:00:00.1"))
                .results(4, dateTime("1970-01-01 00:00:00.1"))

                .param("1970-01-01 00:00:00.12")
                .results(0)
                .results(1)
                .results(2, fillArray(dateTime("1970-01-01 00:00:00.12"), 3))
                .results(3, dateTime("1970-01-01 00:00:00.12"))
                .results(4, dateTime("1970-01-01 00:00:00.12"))

                .param("1970-01-01 00:00:00.123")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))
                .results(4, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))

                .param("1970-01-01 00:00:00.1234")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))
                // TODO https://issues.apache.org/jira/browse/IGNITE-19162 00:00:00.1234 is expected
                .results(4, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))

                // Downcast
                .param("1970-01-01 00:00:00.123")
                .condition(" = {}::TIMESTAMP(2)")
                .results(0)
                .results(1)
                .results(2, fillArray(dateTime("1970-01-01 00:00:00.12"), 3))
                .results(3, dateTime("1970-01-01 00:00:00.12"))
                .results(4, dateTime("1970-01-01 00:00:00.12"))

                .condition(" = {}::TIMESTAMP(1)")
                .results(0)
                .results(1, fillArray(dateTime("1970-01-01 00:00:00.1"), 4))
                .results(2, dateTime("1970-01-01 00:00:00.1"))
                .results(3, dateTime("1970-01-01 00:00:00.1"))
                .results(4, dateTime("1970-01-01 00:00:00.1"))

                .condition(" = {}::TIMESTAMP(0)")
                .results(0, fillArray(dateTime("1970-01-01 00:00:00"), 5))
                .results(1, dateTime("1970-01-01 00:00:00"))
                .results(2, dateTime("1970-01-01 00:00:00"))
                .results(3, dateTime("1970-01-01 00:00:00"))
                .results(4, dateTime("1970-01-01 00:00:00"))

                // Upcast
                .param("1970-01-01 00:00:00.1")
                .condition(" = {}::TIMESTAMP(2)")
                .results(0)
                .results(1, fillArray(dateTime("1970-01-01 00:00:00.1"), 4))
                .results(2, dateTime("1970-01-01 00:00:00.1"))
                .results(3, dateTime("1970-01-01 00:00:00.1"))
                .results(4, dateTime("1970-01-01 00:00:00.1"))

                .condition(" = {}::TIMESTAMP(3)")
                .results(0)
                .results(1, fillArray(dateTime("1970-01-01 00:00:00.1"), 4))
                .results(2, dateTime("1970-01-01 00:00:00.1"))
                .results(3, dateTime("1970-01-01 00:00:00.1"))
                .results(4, dateTime("1970-01-01 00:00:00.1"))
                ;

        // TIMESTAMP WITH LOCAL TIME ZONE
        argsBuilder.type(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .condition(" = {}")
                .param("1970-01-01 00:00:00")
                .results(0, fillArray(instant("1970-01-01 00:00:00"), 5))
                .results(1, instant("1970-01-01 00:00:00"))
                .results(2, instant("1970-01-01 00:00:00"))
                .results(3, instant("1970-01-01 00:00:00"))
                .results(4, instant("1970-01-01 00:00:00"))

                .param("1970-01-01 00:00:00.1")
                .results(0)
                .results(1, fillArray(instant("1970-01-01 00:00:00.1"), 4))
                .results(2, instant("1970-01-01 00:00:00.1"))
                .results(3, instant("1970-01-01 00:00:00.1"))
                .results(4, instant("1970-01-01 00:00:00.1"))

                .param("1970-01-01 00:00:00.12")
                .results(0)
                .results(1)
                .results(2, fillArray(instant("1970-01-01 00:00:00.12"), 3))
                .results(3, instant("1970-01-01 00:00:00.12"))
                .results(4, instant("1970-01-01 00:00:00.12"))

                .param("1970-01-01 00:00:00.123")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(instant("1970-01-01 00:00:00.123"), 2))
                .results(4, fillArray(instant("1970-01-01 00:00:00.123"), 2))

                .param("1970-01-01 00:00:00.1234")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(instant("1970-01-01 00:00:00.123"), 2))
                // TODO https://issues.apache.org/jira/browse/IGNITE-19162 00:00:00.1234 is expected
                .results(4, fillArray(instant("1970-01-01 00:00:00.123"), 2))

                // Downcast
                .param("1970-01-01 00:00:00.123")
                .condition(" = {}::TIMESTAMP(2) WITH LOCAL TIME ZONE")
                .results(0)
                .results(1)
                .results(2, fillArray(instant("1970-01-01 00:00:00.12"), 3))
                .results(3, instant("1970-01-01 00:00:00.12"))
                .results(4, instant("1970-01-01 00:00:00.12"))

                .condition(" = {}::TIMESTAMP(1) WITH LOCAL TIME ZONE")
                .results(0)
                .results(1, fillArray(instant("1970-01-01 00:00:00.1"), 4))
                .results(2, instant("1970-01-01 00:00:00.1"))
                .results(3, instant("1970-01-01 00:00:00.1"))
                .results(4, instant("1970-01-01 00:00:00.1"))

                .condition(" = {}::TIMESTAMP(0) WITH LOCAL TIME ZONE")
                .results(0, fillArray(instant("1970-01-01 00:00:00"), 5))
                .results(1, instant("1970-01-01 00:00:00"))
                .results(2, instant("1970-01-01 00:00:00"))
                .results(3, instant("1970-01-01 00:00:00"))
                .results(4, instant("1970-01-01 00:00:00"))

                // Upcast
                .param("1970-01-01 00:00:00.1")
                .condition(" = {}::TIMESTAMP(2) WITH LOCAL TIME ZONE")
                .results(0)
                .results(1, fillArray(instant("1970-01-01 00:00:00.1"), 4))
                .results(2, instant("1970-01-01 00:00:00.1"))
                .results(3, instant("1970-01-01 00:00:00.1"))
                .results(4, instant("1970-01-01 00:00:00.1"))

                .condition(" = {}::TIMESTAMP(3) WITH LOCAL TIME ZONE")
                .results(0)
                .results(1, fillArray(instant("1970-01-01 00:00:00.1"), 4))
                .results(2, instant("1970-01-01 00:00:00.1"))
                .results(3, instant("1970-01-01 00:00:00.1"))
                .results(4, instant("1970-01-01 00:00:00.1"))
                ;

        return argsBuilder;
    }

    @ParameterizedTest
    @MethodSource("temporalPrecisionGreaterLowerDynParamArgs")
    void testTemporalPrecisionGreaterLowerDynamicParam(SqlTypeName type, String condition, Temporal param, int precision,
            Temporal[] expectedRows) {
        makeCheckerForIndexWithPrecision(type, "sorted", condition, param, precision, expectedRows)
                .check();
    }

    @ParameterizedTest
    @MethodSource("temporalPrecisionGreaterLowerLiteralArgs")
    void testTemporalPrecisionGreaterLowerLiteral(SqlTypeName type, String condition, int precision, Temporal[] expectedRows) {
        makeCheckerForIndexWithPrecision(type, "sorted", condition, null, precision, expectedRows)
                .check();
    }

    private static List<Arguments> temporalPrecisionGreaterLowerDynParamArgs() {
        return temporalPrecisionGreaterLowerArgs().buildDynamicParamsArgs();
    }

    private static List<Arguments> temporalPrecisionGreaterLowerLiteralArgs() {
        return temporalPrecisionGreaterLowerArgs().buildLiteralArgs();
    }

    private static TemporalPrecisionTestArgsBuilder temporalPrecisionGreaterLowerArgs() {
        TemporalPrecisionTestArgsBuilder argsBuilder = new TemporalPrecisionTestArgsBuilder();

        argsBuilder.type(TIME)
                // Greater or equal
                .condition(" >= {}")
                .param("00:00:00.1")
                .results(0)
                .results(1, fillArray(time("00:00:00.1"), 4))
                .results(2, time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.12"), time("00:00:00.12"))
                .results(3, time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))
                .results(4, time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))

                .param("00:00:00.12")
                .results(0)
                .results(1)
                .results(2, fillArray(time("00:00:00.12"), 3))
                .results(3, time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))
                .results(4, time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))

                .param("00:00:00.123")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(time("00:00:00.123"), 2))
                .results(4, fillArray(time("00:00:00.123"), 2))

                .param("00:00:00.1234")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(time("00:00:00.123"), 2))
                .results(4, fillArray(time("00:00:00.123"), 2))

                // Lower or equal
                .condition(" <= {}")
                .param("00:00:00.1")
                .results(0, fillArray(time("00:00:00"), 5))
                .results(1, time("00:00:00"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"))
                .results(2, time("00:00:00"), time("00:00:00.1"))
                .results(3, time("00:00:00"), time("00:00:00.1"))
                .results(4, time("00:00:00"), time("00:00:00.1"))

                .param("00:00:00.12")
                .results(0, fillArray(time("00:00:00"), 5))
                .results(1, time("00:00:00"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"))
                .results(2, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.12"), time("00:00:00.12"))
                .results(3, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"))
                .results(4, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"))

                .param("00:00:00.123")
                .results(0, fillArray(time("00:00:00"), 5))
                .results(1, time("00:00:00"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"))
                .results(2, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.12"), time("00:00:00.12"))
                .results(3, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))
                .results(4, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))

                .param("00:00:00.1234")
                .results(0, fillArray(time("00:00:00"), 5))
                .results(1, time("00:00:00"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"), time("00:00:00.1"))
                .results(2, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.12"), time("00:00:00.12"))
                .results(3, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))
                .results(4, time("00:00:00"), time("00:00:00.1"), time("00:00:00.12"), time("00:00:00.123"), time("00:00:00.123"))
        ;

        // TIMESTAMP
        argsBuilder.type(TIMESTAMP)
                // Greater or equal
                .condition(" >= {}")
                .param("1970-01-01 00:00:00.1")
                .results(0)
                .results(1, fillArray(dateTime("1970-01-01 00:00:00.1"), 4))
                .results(2, dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.12"), dateTime("1970-01-01 00:00:00.12"))
                .results(3, dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))
                .results(4, dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))

                .param("1970-01-01 00:00:00.12")
                .results(0)
                .results(1)
                .results(2, fillArray(dateTime("1970-01-01 00:00:00.12"), 3))
                .results(3, dateTime("1970-01-01 00:00:00.12"), dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))
                .results(4, dateTime("1970-01-01 00:00:00.12"), dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))

                .param("1970-01-01 00:00:00.123")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))
                .results(4, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))

                .param("1970-01-01 00:00:00.1234")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))
                .results(4, fillArray(dateTime("1970-01-01 00:00:00.123"), 2))

                // Lower or equal
                .condition(" <= {}")
                .param("1970-01-01 00:00:00.1")
                .results(0, fillArray(dateTime("1970-01-01 00:00:00"), 5))
                .results(1, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"),
                        dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"))
                .results(2, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"))
                .results(3, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"))
                .results(4, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"))

                .param("1970-01-01 00:00:00.12")
                .results(0, fillArray(dateTime("1970-01-01 00:00:00"), 5))
                .results(1, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"),
                        dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"))
                .results(2, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.12"), dateTime("1970-01-01 00:00:00.12"))
                .results(3, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"))
                .results(4, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"))

                .param("1970-01-01 00:00:00.123")
                .results(0, fillArray(dateTime("1970-01-01 00:00:00"), 5))
                .results(1, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"),
                        dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"))
                .results(2, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.12"), dateTime("1970-01-01 00:00:00.12"))
                .results(3, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))
                .results(4, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))

                .param("1970-01-01 00:00:00.1234")
                .results(0, fillArray(dateTime("1970-01-01 00:00:00"), 5))
                .results(1, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"),
                        dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.1"))
                .results(2, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.12"), dateTime("1970-01-01 00:00:00.12"))
                .results(3, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))
                .results(4, dateTime("1970-01-01 00:00:00"), dateTime("1970-01-01 00:00:00.1"), dateTime("1970-01-01 00:00:00.12"),
                        dateTime("1970-01-01 00:00:00.123"), dateTime("1970-01-01 00:00:00.123"))
                ;

        // TIMESTAMP WITH LOCAL TIME ZONE
        argsBuilder.type(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                // Greater or equal
                .condition(" >= {}")
                .param("1970-01-01 00:00:00.1")
                .results(0)
                .results(1, fillArray(instant("1970-01-01 00:00:00.1"), 4))
                .results(2, instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.12"), instant("1970-01-01 00:00:00.12"))
                .results(3, instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))
                .results(4, instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))

                .param("1970-01-01 00:00:00.12")
                .results(0)
                .results(1)
                .results(2, fillArray(instant("1970-01-01 00:00:00.12"), 3))
                .results(3, instant("1970-01-01 00:00:00.12"), instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))
                .results(4, instant("1970-01-01 00:00:00.12"), instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))

                .param("1970-01-01 00:00:00.123")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(instant("1970-01-01 00:00:00.123"), 2))
                .results(4, fillArray(instant("1970-01-01 00:00:00.123"), 2))

                .param("1970-01-01 00:00:00.1234")
                .results(0)
                .results(1)
                .results(2)
                .results(3, fillArray(instant("1970-01-01 00:00:00.123"), 2))
                .results(4, fillArray(instant("1970-01-01 00:00:00.123"), 2))

                // Lower or equal
                .condition(" <= {}")
                .param("1970-01-01 00:00:00.1")
                .results(0, fillArray(instant("1970-01-01 00:00:00"), 5))
                .results(1, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"),
                        instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"))
                .results(2, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"))
                .results(3, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"))
                .results(4, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"))

                .param("1970-01-01 00:00:00.12")
                .results(0, fillArray(instant("1970-01-01 00:00:00"), 5))
                .results(1, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"),
                        instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"))
                .results(2, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.12"), instant("1970-01-01 00:00:00.12"))
                .results(3, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"))
                .results(4, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"))

                .param("1970-01-01 00:00:00.123")
                .results(0, fillArray(instant("1970-01-01 00:00:00"), 5))
                .results(1, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"),
                        instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"))
                .results(2, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.12"), instant("1970-01-01 00:00:00.12"))
                .results(3, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))
                .results(4, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))

                .param("1970-01-01 00:00:00.1234")
                .results(0, fillArray(instant("1970-01-01 00:00:00"), 5))
                .results(1, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"),
                        instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.1"))
                .results(2, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.12"), instant("1970-01-01 00:00:00.12"))
                .results(3, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))
                .results(4, instant("1970-01-01 00:00:00"), instant("1970-01-01 00:00:00.1"), instant("1970-01-01 00:00:00.12"),
                        instant("1970-01-01 00:00:00.123"), instant("1970-01-01 00:00:00.123"))
                ;

        return argsBuilder;
    }

    private static QueryChecker makeCheckerForIndexWithPrecision(
            SqlTypeName type,
            String idxType,
            String condition,
            @Nullable Temporal param,
            int precision,
            Temporal[] expectedRows
    ) {
        String sortedIdxName = format("t_{}_idx_{}_{}", type.getName(), idxType, precision).toUpperCase();
        String columnName = format("col_{}_{}", type.getName(), precision);
        String tableName = format("t_{}", type.getName()).toUpperCase();
        String query = format("SELECT /*+ FORCE_INDEX({}) */ {} FROM {} WHERE {} {} ORDER BY ID",
                sortedIdxName, columnName, tableName, columnName, condition);

        QueryChecker checker = assertQuery(query)
                .matches(containsIndexScan("PUBLIC", tableName, sortedIdxName))
                .withTimeZoneId(ZoneOffset.UTC);

        if (param != null) {
            checker = checker.withParam(param);
        }

        if (expectedRows.length == 0) {
            return checker.returnNothing();
        }

        for (Temporal expected : expectedRows) {
            checker = checker.returns(expected);
        }

        return checker;
    }

    private static void fillData(String table) {
        switch (table) {
            case "DATE1":
            case "DATE2":
                fill(table, INITIAL_DATE::plusDays);
                break;
            case "TIME1":
            case "TIME2":
                fill(table, INITIAL_TIME::plusSeconds);
                break;
            case "TIMESTAMP1":
            case "TIMESTAMP2":
                fill(table, INITIAL_TS::plusSeconds);
                break;
            case "TIMESTAMPTZ1":
            case "TIMESTAMPTZ2":
                fill(table, INITIAL_TS_LTZ::plusSeconds);
                break;

            case "T_TIME":
                LocalTime baseTime = LocalTime.of(0, 0, 0);

                List<LocalTime> times = List.of(
                        baseTime,
                        baseTime.withNano(100_000_000),
                        baseTime.withNano(120_000_000),
                        baseTime.withNano(123_000_000),
                        baseTime.withNano(123_400_000)
                );

                for (int i = 0; i < times.size(); i++) {
                    sql("INSERT INTO t_time VALUES (?, ?, ?, ?, ?, ?)",
                            i, times.get(i), times.get(i), times.get(i), times.get(i), times.get(i));
                }
                break;

            case "T_TIMESTAMP":
                LocalDateTime baseTs = LocalDateTime.of(1970, Month.JANUARY, 1, 0, 0, 0);

                List<LocalDateTime> dts = List.of(
                        baseTs,
                        baseTs.withNano(100_000_000),
                        baseTs.withNano(120_000_000),
                        baseTs.withNano(123_000_000),
                        baseTs.withNano(123_400_000)
                );

                for (int i = 0; i < dts.size(); i++) {
                    sql("INSERT INTO t_timestamp VALUES (?, ?, ?, ?, ?, ?)", i, dts.get(i), dts.get(i), dts.get(i), dts.get(i), dts.get(i));
                }
                break;

            case "T_TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                LocalDateTime baseTsz = LocalDateTime.of(1970, Month.JANUARY, 1, 0, 0, 0);

                List<Instant> dtsz = List.of(
                        baseTsz.toInstant(ZoneOffset.UTC),
                        baseTsz.withNano(100_000_000).toInstant(ZoneOffset.UTC),
                        baseTsz.withNano(120_000_000).toInstant(ZoneOffset.UTC),
                        baseTsz.withNano(123_000_000).toInstant(ZoneOffset.UTC),
                        baseTsz.withNano(123_400_000).toInstant(ZoneOffset.UTC)
                );

                for (int i = 0; i < dtsz.size(); i++) {
                    sql("INSERT INTO t_timestamp_with_local_time_zone VALUES (?, ?, ?, ?, ?, ?)",
                            i, dtsz.get(i), dtsz.get(i), dtsz.get(i), dtsz.get(i), dtsz.get(i));
                }
                break;

            default:
                throw new IllegalArgumentException("Undefined table: " + table);
        }
    }

    private static void fill(String table, Function<Integer, Object> generator) {
        Object[][] rows = new Object[10][2];

        for (int i = 0; i < 10; ++i) {
            Object val = generator.apply(i);
            rows[i] = new Object[] {val, val};
        }

        insertData(table, List.of("PK", "VAL"), rows);
    }

    private static Temporal[] fillArray(Temporal iteam, int length) {
        Temporal[] res = new Temporal[length];

        Arrays.fill(res, iteam);

        return res;
    }

    private static class TemporalPrecisionTestArgsBuilder {
        private final List<Arguments> dynParamArgs = new ArrayList<>();
        private final List<Arguments> args = new ArrayList<>();
        private SqlTypeName typeName;
        private String condition;
        private String param;

        TemporalPrecisionTestArgsBuilder type(SqlTypeName typeName) {
            this.typeName = typeName;

            return this;
        }

        TemporalPrecisionTestArgsBuilder condition(String condition) {
            this.condition = condition;

            return this;
        }

        TemporalPrecisionTestArgsBuilder param(String param) {
            this.param = param;

            return this;
        }

        TemporalPrecisionTestArgsBuilder results(int precision, Temporal ... expectedRows) {
            String dynParamCondition = format(condition, "?");
            String literalCondition = format(condition, typeName.getSpaceName() + " '" + param + "'");

            Temporal dynParam = null;

            if (typeName == TIME) {
                dynParam = LocalTime.parse(param);
            } else if (typeName == TIMESTAMP) {
                dynParam = dateTime(param);
            } else if (typeName == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                dynParam = instant(param);
            }

            assertNotNull(dynParam);

            dynParamArgs.add(Arguments.of(typeName, dynParamCondition, dynParam, precision, expectedRows));
            args.add(Arguments.of(typeName, literalCondition, precision, expectedRows));

            return this;
        }

        List<Arguments> buildDynamicParamsArgs() {
            return dynParamArgs;
        }

        List<Arguments> buildLiteralArgs() {
            return args;
        }
    }

    static class Parser {
        static LocalDateTime dateTime(String s) {
            return LocalDateTime.parse(s.replace(' ', 'T'));
        }

        static Instant instant(String s) {
            return dateTime(s).toInstant(ZoneOffset.UTC);
        }

        static LocalTime time(String s) {
            return LocalTime.parse(s);
        }
    }
}
