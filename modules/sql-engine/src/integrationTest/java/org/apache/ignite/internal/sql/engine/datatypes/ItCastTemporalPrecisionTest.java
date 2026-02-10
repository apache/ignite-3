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

package org.apache.ignite.internal.sql.engine.datatypes;

import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.datatypes.ItCastTemporalPrecisionTest.Parser.dateTime;
import static org.apache.ignite.internal.sql.engine.datatypes.ItCastTemporalPrecisionTest.Parser.instant;
import static org.apache.ignite.internal.sql.engine.datatypes.ItCastTemporalPrecisionTest.Parser.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.ListOfListsMatcher;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * End-to-End tests that verify precision of the result of a CAST on temporal types.
 */
public class ItCastTemporalPrecisionTest extends BaseSqlIntegrationTest {
    /** List of tested precisions. */
    private static final List<Integer> PRECISIONS = List.of(0, 1, 2, 3, 6, 9);

    /** Rows counter. */
    private final AtomicInteger rowsCounter = new AtomicInteger(1);

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void createTables() {
        IgniteStringBuilder scriptBuf = new IgniteStringBuilder();

        // Creates four tables for each specified type (T_TIME, T_TIMESTAMP...).
        // With seven columns- for each tested precision (0, 1, ...) and PK (int) column.
        // The suffix number of the column name is equal to the precision.
        // For example T_TIME:
        //    ID INT, C0 TIME(0). C1 TIME(1)...
        List.of(TIME, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, VARCHAR)
                .forEach(type -> {
                    String columns = PRECISIONS.stream()
                            .map(p -> ", c" + p + " " + sqlNameWithPrecision(type, type == VARCHAR ? 64 : p)
                    ).collect(Collectors.joining());

                    scriptBuf.app(format("CREATE TABLE t_{} (ID INT PRIMARY KEY{});\n", type.getName(), columns));
                });

        String script = scriptBuf.toString();

        log.info("Executing setup script:\n" + script);

        CLUSTER.aliveNode().sql().executeScript(script);
    }

    /**
     * Test verifies precision of the result of a CAST from literal (and dynamic parameter) to the specified type.
     */
    @ParameterizedTest(name = "{0} ''{1}''::{2}({3}) = {4}")
    @MethodSource("selectCastArgs")
    public void selectCast(SqlTypeName sourceType, String literal, SqlTypeName targetType, int targetPrecision,
            Matcher<List<List<?>>> matcher) {
        RelDataType targetDataType = Commons.typeFactory().createSqlType(targetType, targetPrecision);
        ColumnType expectColumnType = TypeUtils.columnType(targetDataType);
        String literalType = sourceType == VARCHAR ? "" : sourceType.getSpaceName();
        String targetTypeString = sqlNameWithPrecision(targetType, targetPrecision);

        // Literal.
        {
            assertQuery(format("SELECT {} '{}'::{}", literalType, literal, targetTypeString))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .columnMetadata(new MetadataMatcher().type(expectColumnType).precision(targetPrecision))
                    .results(matcher)
                    .check();
        }

        // Literal with format.
        {
            String query = format("SELECT CAST({} '{}' AS {} FORMAT '{}')",
                    literalType, literal, targetTypeString, sqlFormat(targetType));

            assertQuery(query)
                    .withTimeZoneId(ZoneOffset.UTC)
                    .columnMetadata(new MetadataMatcher().type(expectColumnType).precision(targetPrecision))
                    .results(matcher)
                    .check();
        }

        // Dynamic parameter.
        {
            Object param = parseSourceLiteral(literal, sourceType);

            assertQuery(format("SELECT ?::{}", sqlNameWithPrecision(targetType, targetPrecision)))
                    .withParam(param)
                    .withTimeZoneId(ZoneOffset.UTC)
                    .columnMetadata(new MetadataMatcher().type(expectColumnType).precision(targetPrecision))
                    .results(matcher)
                    .check();
        }
    }

    private static Stream<Arguments> selectCastArgs() {
        return castArgs().map(SelectArgs::toArgs);
    }

    /**
     * The test verifies the precision of the CAST result when inserting/updating from one table column to another.
     */
    @ParameterizedTest(name = "{0}({1}) {2} :: {3}({4}) = {5}")
    @MethodSource("dmlCastArgs")
    public void dmlCast(SqlTypeName sourceType, int sourcePrecision, String literal, SqlTypeName targetType, int targetPrecision,
            Matcher<List<List<?>>> matcher) {
        String sourceColumnName = "c" + sourcePrecision;
        String targetColumnName = "c" + targetPrecision;

        // UPSERT initial literal value (id = 0).
        {
            int sourceCastPrecision = 9;

            // The source column for the VARCHAR has a "precision" of 64, and when inserted into the
            // source column, the value from the literal will not be truncated to match the source precision.
            // Therefore, we add an explicit conversion with truncation of the literal value to the required precision.
            if (sourceType == VARCHAR) {
                sourceCastPrecision = sourcePrecision == 0
                        ? literal.length() - 10
                        : literal.length() - 9 + sourcePrecision;
            }

            // Upsert the provided literal value to a column (that has specified precision) in the source table.
            String query = format("MERGE INTO t_{} dst "
                            + "USING ("
                            + "  SELECT 0 AS ID, '{}'::{} as {}"
                            + ") as src ON dst.id=src.id "
                            + "WHEN MATCHED THEN UPDATE SET dst.{}=src.{} "
                            + "WHEN NOT MATCHED THEN INSERT (id, {}) VALUES (src.id, src.{})",
                    sourceType.getName(), literal, sqlNameWithPrecision(sourceType, sourceCastPrecision),
                    sourceColumnName, sourceColumnName, sourceColumnName, sourceColumnName, sourceColumnName);

            assertQuery(query)
                    .withTimeZoneId(ZoneOffset.UTC)
                    .returnSomething()
                    .check();
        }

        RelDataType targetDataType = Commons.typeFactory().createSqlType(targetType, targetPrecision);
        ColumnType expectColumnType = TypeUtils.columnType(targetDataType);
        String expr = sourceType.getFamily() == targetType.getFamily()
                ? sourceColumnName
                : format("{}::{}", sourceColumnName, sqlNameWithPrecision(targetType, targetPrecision));

        int rowNum = rowsCounter.incrementAndGet();

        // Insert value from the source table (with id = 0) to target table column.
        {
            assertQuery(format("INSERT INTO t_{} (ID, {}) SELECT {}, {} FROM t_{} WHERE id=0",
                    targetType.getName(), targetColumnName, rowNum, expr, sourceType.getName()))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .returnSomething()
                    .check();

            assertQuery(format("SELECT {} FROM t_{} WHERE id={}", targetColumnName, targetType.getName(), rowNum))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .columnMetadata(new MetadataMatcher().type(expectColumnType).precision(targetPrecision))
                    .results(matcher)
                    .check();
        }

        // Update value in the target column.
        {
            assertQuery(format("UPDATE t_{} t0 SET t0.{}=(SELECT {} FROM t_{} WHERE id=0) WHERE t0.id={}",
                    targetType.getName(), targetColumnName, expr, sourceType.getName(), rowNum))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .returnSomething()
                    .check();

            assertQuery(format("SELECT {} FROM t_{} WHERE id={}", targetColumnName, targetType.getName(), rowNum))
                    .withTimeZoneId(ZoneOffset.UTC)
                    .columnMetadata(new MetadataMatcher().type(expectColumnType).precision(targetPrecision))
                    .results(matcher)
                    .check();
        }
    }

    private static Stream<Arguments> dmlCastArgs() {
        return castArgs()
                .flatMap(args -> PRECISIONS.stream()
                        .map(sourcePrecision -> new DmlArgs(args, sourcePrecision).toArgs())
                );
    }

    private static Stream<SelectArgs> castArgs() {
        return Stream.of(
                // VARCHAR => TIME
                new SelectArgs(VARCHAR, "00:00:00.999999999", TIME, 0, time("00:00:00")),
                new SelectArgs(VARCHAR, "00:00:00.999999999", TIME, 1, time("00:00:00.9")),
                new SelectArgs(VARCHAR, "00:00:00.999999999", TIME, 2, time("00:00:00.99")),
                new SelectArgs(VARCHAR, "00:00:00.999999999", TIME, 3, time("00:00:00.999")),
                new SelectArgs(VARCHAR, "00:00:00.999999999", TIME, 6, time("00:00:00.999")),
                new SelectArgs(VARCHAR, "00:00:00.999999999", TIME, 9, time("00:00:00.999")),

                // VARCHAR => TIMESTAMP
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999", TIMESTAMP, 0, dateTime("2024-01-01 00:00:00")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999", TIMESTAMP, 1, dateTime("2024-01-01 00:00:00.9")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999", TIMESTAMP, 2, dateTime("2024-01-01 00:00:00.99")),
                new SelectArgs(VARCHAR, "0024-01-01 00:00:00.999999999", TIMESTAMP, 3, dateTime("0024-01-01 00:00:00.999")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999", TIMESTAMP, 6, dateTime("2024-01-01 00:00:00.999")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999", TIMESTAMP, 9, dateTime("2024-01-01 00:00:00.999")),

                // VARCHAR => TIMESTAMP_LTZ
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0, instant("2024-01-01 00:00:00")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 1, instant("2024-01-01 00:00:00.9")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 2, instant("2024-01-01 00:00:00.99")),
                new SelectArgs(VARCHAR, "0024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3, instant("0024-01-01 00:00:00.999")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 6, instant("2024-01-01 00:00:00.999")),
                new SelectArgs(VARCHAR, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9, instant("2024-01-01 00:00:00.999")),

                // TIMESTAMP => TIME
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIME, 0, time("00:00:00")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIME, 1, time("00:00:00.9")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIME, 2, time("00:00:00.99")),
                new SelectArgs(TIMESTAMP, "0024-01-01 00:00:00.999999999", TIME, 3, time("00:00:00.999")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIME, 6, time("00:00:00.999")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIME, 9, time("00:00:00.999")),

                // TIMESTAMP => TIMESTAMP
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIMESTAMP, 0, dateTime("2024-01-01 00:00:00")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIMESTAMP, 1, dateTime("2024-01-01 00:00:00.9")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIMESTAMP, 2, dateTime("2024-01-01 00:00:00.99")),
                new SelectArgs(TIMESTAMP, "0024-01-01 00:00:00.999999999", TIMESTAMP, 3, dateTime("0024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIMESTAMP, 6, dateTime("2024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999", TIMESTAMP, 9, dateTime("2024-01-01 00:00:00.999")),

                // TIMESTAMP => TIMESTAMP_LTZ
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0, instant("2024-01-01 00:00:00")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 1, instant("2024-01-01 00:00:00.9")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 2, instant("2024-01-01 00:00:00.99")),
                new SelectArgs(TIMESTAMP, "0024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3, instant("0024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 6, instant("2024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9, instant("2024-01-01 00:00:00.999")),

                // TIMESTAMP_LTZ => TIME
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999", TIME, 0, time("00:00:00")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999", TIME, 1, time("00:00:00.9")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999", TIME, 2, time("00:00:00.99")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "0024-01-01 00:00:00.999999999", TIME, 3, time("00:00:00.999")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999", TIME, 6, time("00:00:00.999")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999", TIME, 9, time("00:00:00.999")),

                // TIMESTAMP_LTZ => TIMESTAMP
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP, 0, dateTime("2024-01-01 00:00:00")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP, 1, dateTime("2024-01-01 00:00:00.9")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP, 2, dateTime("2024-01-01 00:00:00.99")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "0024-01-01 00:00:00.999999999",
                        TIMESTAMP, 3, dateTime("0024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP, 6, dateTime("2024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP, 9, dateTime("2024-01-01 00:00:00.999")),

                // TIMESTAMP_LTZ => TIMESTAMP_LTZ
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0, instant("2024-01-01 00:00:00")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 1, instant("2024-01-01 00:00:00.9")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 2, instant("2024-01-01 00:00:00.99")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "0024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3, instant("0024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 6, instant("2024-01-01 00:00:00.999")),
                new SelectArgs(TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-01 00:00:00.999999999",
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9, instant("2024-01-01 00:00:00.999")),

                // TIME => TIME
                new SelectArgs(TIME, "00:00:00.999999999", TIME, 0, time("00:00:00")),
                new SelectArgs(TIME, "00:00:00.999999999", TIME, 1, time("00:00:00.9")),
                new SelectArgs(TIME, "00:00:00.999999999", TIME, 2, time("00:00:00.99")),
                new SelectArgs(TIME, "00:00:00.999999999", TIME, 3, time("00:00:00.999")),
                new SelectArgs(TIME, "00:00:00.999999999", TIME, 6, time("00:00:00.999")),
                new SelectArgs(TIME, "00:00:00.999999999", TIME, 9, time("00:00:00.999")),

                // TIME => TIMESTAMP
                // NOTE: to reduce the complexity of the test, the DATE in the expected value
                //       is completely ignored and only the TIME is checked (see TimeMatcher).
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP, 0, dateTime("0000-01-01 00:00:00")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP, 1, dateTime("0000-01-01 00:00:00.9")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP, 2, dateTime("0000-01-01 00:00:00.99")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP, 3, dateTime("0000-01-01 00:00:00.999")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP, 6, dateTime("0000-01-01 00:00:00.999")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP, 9, dateTime("0000-01-01 00:00:00.999")),

                // TIME => TIMESTAMP_WITH_LOCAL_TIME_ZONE
                // NOTE: to reduce the complexity of the test, the DATE in the expected value
                //       is completely ignored and only the TIME is checked (see TimeMatcher).
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0, instant("0000-01-01 00:00:00")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 1, instant("0000-01-01 00:00:00.9")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 2, instant("0000-01-01 00:00:00.99")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3, instant("0000-01-01 00:00:00.999")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 6, instant("0000-01-01 00:00:00.999")),
                new SelectArgs(TIME, "00:00:00.999999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9, instant("0000-01-01 00:00:00.999"))
        );
    }

    @ParameterizedTest(name = "SELECT {1} ''{0}'' = {3} ({1}({2}))")
    @MethodSource("selectLiteralArgs")
    public void selectLiteral(String literal, SqlTypeName targetType, int precision, Temporal expTemporal) {
        RelDataType targetDataType = Commons.typeFactory().createSqlType(targetType, precision);
        ColumnType expectColumnType = TypeUtils.columnType(targetDataType);

        assertQuery(format("SELECT {} '{}'", targetType.getSpaceName(), literal))
                .withTimeZoneId(ZoneOffset.UTC)
                .returns(expTemporal)
                .columnMetadata(new MetadataMatcher().type(expectColumnType).precision(precision))
                .check();
    }

    private static Stream<Arguments> selectLiteralArgs() {
        return Stream.of(
                // TIME
                Arguments.of("00:00:00", TIME, 0, time("00:00:00")),
                Arguments.of("00:00:00.9", TIME, 1, time("00:00:00.9")),
                Arguments.of("00:00:00.99", TIME, 2, time("00:00:00.99")),
                Arguments.of("00:00:00.999", TIME, 3, time("00:00:00.999")),
                Arguments.of("00:00:00.999999", TIME, 6, time("00:00:00.999")),
                Arguments.of("00:00:00.999999999", TIME, 9, time("00:00:00.999")),

                // TIMESTAMP
                Arguments.of("2024-01-01 00:00:00", TIMESTAMP, 0, dateTime("2024-01-01 00:00:00")),
                Arguments.of("2024-01-01 00:00:00.9", TIMESTAMP, 1, dateTime("2024-01-01 00:00:00.9")),
                Arguments.of("2024-01-01 00:00:00.99", TIMESTAMP, 2, dateTime("2024-01-01 00:00:00.99")),
                Arguments.of("2024-01-01 00:00:00.999", TIMESTAMP, 3, dateTime("2024-01-01 00:00:00.999")),
                Arguments.of("2024-01-01 00:00:00.999999", TIMESTAMP, 6, dateTime("2024-01-01 00:00:00.999")),
                Arguments.of("2024-01-01 00:00:00.999999999", TIMESTAMP, 9, dateTime("2024-01-01 00:00:00.999")),

                // TIMESTAMP LTZ
                Arguments.of("2024-01-01 00:00:00", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0, instant("2024-01-01 00:00:00")),
                Arguments.of("2024-01-01 00:00:00.9", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 1, instant("2024-01-01 00:00:00.9")),
                Arguments.of("2024-01-01 00:00:00.99", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 2, instant("2024-01-01 00:00:00.99")),
                Arguments.of("2024-01-01 00:00:00.999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3, instant("2024-01-01 00:00:00.999")),
                Arguments.of("2024-01-01 00:00:00.999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 6, instant("2024-01-01 00:00:00.999")),
                Arguments.of("2024-01-01 00:00:00.999999999", TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9, instant("2024-01-01 00:00:00.999"))
        );
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

    private static String sqlNameWithPrecision(SqlTypeName type, int precision) {
        if (type == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return format("TIMESTAMP({}) WITH LOCAL TIME ZONE ", precision);
        }

        return type.getName() + "(" + precision + ")";
    }

    private static String sqlFormat(SqlTypeName type) {
        if (Objects.requireNonNull(type) == TIME) {
            return "hh24:mi:ss.ff9";
        }

        return "yyyy-MM-dd hh24:mi:ss.ff9";
    }

    private static Object parseSourceLiteral(String literal, SqlTypeName sourceType) {
        switch (sourceType) {
            case TIME:
                return LocalTime.parse(literal);

            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalDateTime dateTime = dateTime(literal.replace(' ', 'T'));
                return sourceType == TIMESTAMP ? dateTime : dateTime.toInstant(ZoneOffset.UTC);

            case VARCHAR:
                return literal;

            default:
                throw new IllegalStateException("Unexpected value: " + sourceType);
        }
    }

    static class SelectArgs {
        final SqlTypeName sourceType;
        final String literal;
        final SqlTypeName targetType;
        final int targetPrecision;
        final Temporal expected;
        final Matcher<List<List<?>>> matcher;

        SelectArgs(SqlTypeName sourceType, String literal, SqlTypeName targetType, int targetPrecision, Temporal expected) {
            this.sourceType = sourceType;
            this.literal = literal;
            this.targetType = targetType;
            this.targetPrecision = targetPrecision;
            this.expected = expected;
            this.matcher = sourceType == TIME
                    ? new ListOfListsMatcher(Matchers.contains(new TimeMatcher(expected)))
                    : new ListOfListsMatcher(Matchers.contains(expected));
        }

        Arguments toArgs() {
            return Arguments.of(sourceType, literal, targetType, targetPrecision, matcher);
        }
    }

    static class DmlArgs extends SelectArgs {
        private final int sourcePrecision;

        DmlArgs(SelectArgs args, int sourcePrecision) {
            super(args.sourceType, args.literal, args.targetType, args.targetPrecision,
                    SqlTestUtils.adjustTemporalPrecision(TypeUtils.columnType(Commons.typeFactory().createSqlType(args.targetType)),
                            args.expected, Math.min(sourcePrecision, args.targetPrecision)));

            this.sourcePrecision = sourcePrecision;
        }

        @Override
        Arguments toArgs() {
            return Arguments.of(sourceType, sourcePrecision, literal, targetType, targetPrecision, matcher);
        }
    }

    static class TimeMatcher extends BaseMatcher<Object> {
        private final LocalTime expected;

        TimeMatcher(Temporal expected) {
            this.expected = extractTime(expected);
        }

        @Override
        public boolean matches(Object obj) {
            LocalTime actual = extractTime(obj);

            return actual.equals(expected);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("must have expected time " + expected);
        }

        static LocalTime extractTime(Object obj) {
            if (obj instanceof LocalTime) {
                return (LocalTime) obj;
            }

            if (obj instanceof LocalDateTime) {
                return ((LocalDateTime) obj).toLocalTime();
            }

            if (obj instanceof Instant) {
                return LocalDateTime.ofInstant(((Instant) obj), ZoneOffset.UTC).toLocalTime();
            }

            throw new IllegalArgumentException("Unsupported type " + obj);
        }

        @Override
        public String toString() {
            return String.valueOf(expected);
        }
    }
}
