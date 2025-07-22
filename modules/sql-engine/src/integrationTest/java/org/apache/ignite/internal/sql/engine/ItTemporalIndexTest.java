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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.matchesOnce;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.SQL_CONFORMANT_DATETIME_FORMATTER;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
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
            "DATE1", new String[]{"S_ASC_IDX_DATE1", "S_DESC_IDX_DATE1"},
            "TIME1", new String[]{"S_ASC_IDX_TIME1", "S_DESC_IDX_TIME1"},
            "TIMESTAMP1", new String[]{"S_ASC_IDX_TIMESTAMP1", "S_DESC_IDX_TIMESTAMP1"},
            "TIMESTAMPTZ1", new String[]{"S_ASC_IDX_TZ1", "S_DESC_IDX_TZ1"}
    );

    @BeforeAll
    static void initTestData() {
        String query = new IgniteStringBuilder()
                .app("CREATE TABLE DATE1 (pk DATE, val DATE, PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE DATE2 (pk DATE, val DATE, PRIMARY KEY USING HASH (pk));").nl()
                .app("CREATE TABLE TIME1 (pk TIME, val TIME, PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE TIME2 (pk TIME, val TIME, PRIMARY KEY USING HASH (pk));").nl()
                .app("CREATE TABLE TIMESTAMP1 (pk TIMESTAMP, val TIMESTAMP, PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE TIMESTAMP2 (pk TIMESTAMP(0), val TIMESTAMP, PRIMARY KEY USING HASH (pk));").nl()
                .app("CREATE TABLE TIMESTAMPTZ1 (pk TIMESTAMP WITH LOCAL TIME ZONE, val TIMESTAMP WITH LOCAL TIME ZONE, "
                        + "PRIMARY KEY USING SORTED (pk));").nl()
                .app("CREATE TABLE TIMESTAMPTZ2 (pk TIMESTAMP(0) WITH LOCAL TIME ZONE, val TIMESTAMP WITH LOCAL TIME ZONE, "
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
        assertQuery(format("SELECT /*+ FORCE_INDEX({}), DISABLE_RULE('TableScanToKeyValueGetRule') */ val FROM {} WHERE pk {} ORDER BY val",
                pkIndexName(table), table, predicate))
                .matches(containsIndexScan("PUBLIC", table, pkIndexName(table)))
                .returns(result)
                .check();

        for (String idx : SORTED_INDEXES.get(table)) {
            assertQuery(format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk {} ORDER BY val",
                    idx, table, predicate))
                    .matches(containsIndexScan("PUBLIC", table, idx))
                    .returns(result)
                    .check();
        }
    }

    /** Check range predicates with dynamic parameter. */
    @ParameterizedTest(name = "table = {0}, predicate = {1}")
    @MethodSource("geLeSearchDynParamArguments")
    public void testSearchGtLtWithPkIdxUsageDynamicParam(String table, String predicate, Object parameter, Object result) {
        assertQuery(format("SELECT /*+ FORCE_INDEX({}), DISABLE_RULE('TableScanToKeyValueGetRule') */ val FROM {} WHERE pk {} ORDER BY val",
                pkIndexName(table), table, predicate))
                .withParam(parameter)
                .matches(containsIndexScan("PUBLIC", table, pkIndexName(table)))
                .returns(result)
                .check();

        for (String idx : SORTED_INDEXES.get(table)) {
            assertQuery(format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk {} ORDER BY val",
                    idx, table, predicate))
                    .withParam(parameter)
                    .matches(containsIndexScan("PUBLIC", table, idx))
                    .returns(result)
                    .check();
        }
    }

    /** Check range predicates. */
    @ParameterizedTest(name = "table = {0}, predicate = {1}")
    @MethodSource("betweenSearchArguments")
    public void testBetweenPkIdxUsage(String table, String predicate, Object[] result) {
        QueryChecker checker = assertQuery(
                format("SELECT /*+ FORCE_INDEX({})*/ val FROM {} WHERE pk {}",
                        pkIndexName(table), table, predicate))
                .matches(containsIndexScan("PUBLIC", table, pkIndexName(table)));

        for (Object res : result) {
            checker.returns(res);
        }

        checker.check();

        for (String idx : SORTED_INDEXES.get(table)) {
            checker = assertQuery(
                    format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk {}", idx, table, predicate))
                    .matches(containsIndexScan("PUBLIC", table, idx));

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
        QueryChecker checker = assertQuery(
                format("SELECT /*+ FORCE_INDEX({})*/ val FROM {} WHERE pk BETWEEN ? AND ?",
                        pkIndexName(table), table))
                .withParams(params)
                .matches(containsIndexScan("PUBLIC", table, pkIndexName(table)));

        for (Object res : result) {
            checker.returns(res);
        }

        checker.check();

        for (String idx : SORTED_INDEXES.get(table)) {
            checker = assertQuery(
                    format("SELECT /*+ FORCE_INDEX({}) */ val FROM {} WHERE pk BETWEEN ? AND ?", idx, table))
                    .withParams(params)
                    .matches(containsIndexScan("PUBLIC", table, idx));

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

                Arguments.of("TIMESTAMPTZ1", " > ?", INITIAL_TS.plusSeconds(8), INITIAL_TS_LTZ.plusSeconds(9)),
                Arguments.of("TIMESTAMPTZ1", " >= ?", INITIAL_TS.plusSeconds(9), INITIAL_TS_LTZ.plusSeconds(9)),
                Arguments.of("TIMESTAMPTZ1", " < ?", INITIAL_TS.plusSeconds(1), INITIAL_TS_LTZ),
                Arguments.of("TIMESTAMPTZ1", " <= ?", INITIAL_TS, INITIAL_TS_LTZ)
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

    @Override
    protected int initialNodes() {
        return 1;
    }
}
