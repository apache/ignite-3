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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.EXACT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.generateValueByType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.fromInternal;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand.PrimaryKeyIndexType;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * For {@link DdlSqlToCommandConverter} testing.
 */
public class DdlSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {
    @Test
    void testCheckDuplicates() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> checkDuplicates(
                        Set.of("replicas", "affinity"),
                        Set.of("partitions", "replicas")
                )
        );

        assertThat(exception.getMessage(), startsWith("Duplicate id: replicas"));

        assertDoesNotThrow(() -> checkDuplicates(
                        Set.of("replicas", "affinity"),
                        Set.of("replicas0", "affinity0")
                )
        );
    }

    @Test
    public void tableWithoutPkShouldThrowErrorWhenSysPropDefault() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) WITH STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("Table without PRIMARY KEY is not supported"));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "false")
    public void tableWithoutPkShouldThrowErrorWhenSysPropDisabled() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) WITH STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("Table without PRIMARY KEY is not supported"));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithoutPkShouldInjectImplicitPkWhenSysPropEnabled() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) WITH STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertThat(
                createTable.columns(),
                allOf(
                        hasItem(columnThat("column with name \"VAL\"", cd -> "VAL".equals(cd.name()))),
                        hasItem(columnThat("implicit PK col", cd -> Commons.IMPLICIT_PK_COL_NAME.equals(cd.name())
                                && !cd.nullable() && SqlTypeName.VARCHAR.equals(cd.type().getSqlTypeName())))
                )
        );

        assertThat(
                createTable.primaryKeyColumns(),
                hasSize(1)
        );

        assertThat(
                createTable.primaryKeyColumns(),
                hasItem(Commons.IMPLICIT_PK_COL_NAME)
        );

        assertEquals(createTable.primaryIndexType(), PrimaryKeyIndexType.HASH);
    }

    @ParameterizedTest
    @CsvSource({
            "ASC, ASC_NULLS_LAST",
            "DESC, DESC_NULLS_FIRST"
    })
    public void tableWithSortedPk(String sqlCol, Collation collation) throws SqlParseException {
        String query = format("CREATE TABLE t (id int, val int, PRIMARY KEY USING SORTED (id {}))", sqlCol);
        var node = parse(query);

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertEquals(createTable.primaryIndexType(), PrimaryKeyIndexType.SORTED);
        assertEquals(createTable.primaryKeyColumns(), List.of("ID"));
        assertEquals(createTable.primaryKeyCollations(), List.of(collation));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE t (c1 int PRIMARY KEY, c2 int PRIMARY KEY, c3 int)",
            "CREATE TABLE t (c1 int, c2 int, c3 int, PRIMARY KEY (c1), PRIMARY KEY (c1) )",
            "CREATE TABLE t (c1 int, c2 int, c3 int, PRIMARY KEY (c1), PRIMARY KEY (c2) )",
    })
    public void tablePkAppearsOnlyOnce(String stmt) throws SqlParseException {
        var node = parse(stmt);
        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(STMT_VALIDATION_ERR,
                "Unexpected number of primary key constraints [expected at most one, but was 2",
                () -> converter.convert((SqlDdl) node, createContext())
        );
    }

    @Test
    public void tableWithHashPk() throws SqlParseException {
        var node = parse("CREATE TABLE t (id int, val int, PRIMARY KEY USING HASH (id))");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertEquals(createTable.primaryIndexType(), PrimaryKeyIndexType.HASH);
        assertEquals(createTable.primaryKeyColumns(), List.of("ID"));
        assertEquals(createTable.primaryKeyCollations(), Collections.emptyList());
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithIdentifierZone() throws SqlParseException {
        var node = parse("CREATE TABLE t (id int) WITH PRIMARY_ZONE=test_zone");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertEquals("TEST_ZONE", createTable.zone());
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithLiteralZone() throws SqlParseException {
        var node = parse("CREATE TABLE t (id int) WITH PRIMARY_ZONE='test_zone'");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertEquals("test_zone", createTable.zone());
    }

    @SuppressWarnings({"ThrowableNotThrown"})
    @TestFactory
    public Stream<DynamicTest> numericDefaultWithIntervalTypes() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        for (SqlTypeName numType : NUMERIC_TYPES) {
            for (SqlTypeName intervalType : INTERVAL_TYPES) {
                RelDataType initialNumType = Commons.typeFactory().createSqlType(numType);
                ColumnType colType = columnType(initialNumType);
                Object value = generateValueByType(1000, Objects.requireNonNull(colType));
                String intervalTypeStr = makeUsableIntervalType(intervalType.getName());

                fillTestCase(intervalTypeStr, "" + value, testItems, false, ctx);
                fillTestCase(intervalTypeStr, "'" + value + "'", testItems, false, ctx);
            }
        }

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> intervalDefaultsWithNumericTypes() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        for (SqlTypeName intervalType : INTERVAL_TYPES) {
            for (SqlTypeName numType : NUMERIC_TYPES) {
                String value = makeUsableIntervalValue(intervalType.getName());

                fillTestCase(numType.getName(), value, testItems, false, ctx);
            }
        }

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> nonIntervalDefaultsWithIntervalTypes() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        String[] values = {"'01:01:02'", "'2020-01-02 01:01:01'", "'2020-01-02'", "true", "'true'", "x'01'"};

        for (String value : values) {
            for (SqlTypeName intervalType : INTERVAL_TYPES) {
                fillTestCase(makeUsableIntervalType(intervalType.getName()), value, testItems, false, ctx);
            }
        }

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> intervalDefaultsWithIntervalTypes() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        assertEquals(Period.of(1, 1, 0), fromInternal(13, Period.class));
        assertEquals(Period.of(1, 0, 0), fromInternal(12, Period.class));

        fillTestCase("INTERVAL YEARS", "INTERVAL '1' YEAR", testItems, true, ctx, fromInternal(12, Period.class));
        fillTestCase("INTERVAL YEARS", "INTERVAL '12' MONTH", testItems, true, ctx, fromInternal(12, Period.class));
        fillTestCase("INTERVAL YEARS TO MONTHS", "INTERVAL '1' YEAR", testItems, true, ctx, fromInternal(12, Period.class));
        fillTestCase("INTERVAL YEARS TO MONTHS", "INTERVAL '13' MONTH", testItems, true, ctx, fromInternal(13, Period.class));
        fillTestCase("INTERVAL MONTHS", "INTERVAL '1' YEAR", testItems, true, ctx, fromInternal(12, Period.class));
        fillTestCase("INTERVAL MONTHS", "INTERVAL '13' MONTHS", testItems, true, ctx, fromInternal(13, Period.class));

        long oneDayMillis = Duration.ofDays(1).toMillis();
        long oneHourMillis = Duration.ofHours(1).toMillis();
        long oneMinuteMillis = Duration.ofMinutes(1).toMillis();
        long oneSecondMillis = Duration.ofSeconds(1).toMillis();

        fillTestCase("INTERVAL DAYS", "INTERVAL '1' DAY", testItems, true, ctx, fromInternal(oneDayMillis, Duration.class));
        fillTestCase("INTERVAL DAYS TO HOURS", "INTERVAL '1' HOURS", testItems, true, ctx, fromInternal(oneHourMillis, Duration.class));
        fillTestCase("INTERVAL HOURS TO SECONDS", "INTERVAL '1' MINUTE", testItems, true, ctx,
                fromInternal(oneMinuteMillis, Duration.class));
        fillTestCase("INTERVAL MINUTES TO SECONDS", "INTERVAL '1' MINUTE", testItems, true, ctx,
                fromInternal(oneMinuteMillis, Duration.class));
        fillTestCase("INTERVAL MINUTES TO SECONDS", "INTERVAL '1' SECOND", testItems, true, ctx,
                fromInternal(oneSecondMillis, Duration.class));

        return testItems.stream();
    }

    @SuppressWarnings({"ThrowableNotThrown"})
    @Test
    public void testUuidWithDefaults() throws SqlParseException {
        PlanningContext ctx = createContext();
        String template = "CREATE TABLE t (id INTEGER PRIMARY KEY, d UUID DEFAULT {})";

        String sql = format(template, "NULL");
        CreateTableCommand cmd = (CreateTableCommand) converter.convert((SqlDdl) parse(sql), ctx);
        ColumnDefinition def = cmd.columns().get(1);
        DefaultValueDefinition.ConstantValue defVal = def.defaultValueDefinition();
        assertNull(defVal.value());

        UUID uuid = UUID.randomUUID();
        sql = format(template, "'" + uuid + "'");
        cmd = (CreateTableCommand) converter.convert((SqlDdl) parse(sql), ctx);
        def = cmd.columns().get(1);
        defVal = def.defaultValueDefinition();
        assertEquals(uuid, defVal.value());

        String[] values = {"'01:01:02'", "'2020-01-02 01:01:01'", "'2020-01-02'", "true", "'true'", "x'01'", "INTERVAL '1' DAY"};
        for (String value : values) {
            String sql0 = format(template, value);
            assertThrowsSqlException(STMT_VALIDATION_ERR, "Invalid default value for column", () ->
                    converter.convert((SqlDdl) parse(sql0), ctx));
        }
    }

    @TestFactory
    public Stream<DynamicTest> numericTypesWithNumericDefaults() {
        Pattern exactNumeric = Pattern.compile("^\\d+$");
        Pattern numeric = Pattern.compile("^\\d+(\\.{1}\\d*)?$");
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        String[] numbers = {"100.4", "100.6", "100", "'100'", "'100.1'"};

        List<SqlTypeName> typesWithoutDecimal = new ArrayList<>(NUMERIC_TYPES);
        typesWithoutDecimal.remove(DECIMAL);

        for (String value : numbers) {
            for (SqlTypeName numericType : typesWithoutDecimal) {
                Object toCompare = null;
                boolean acceptable = true;

                if (!numeric.matcher(value).matches()) {
                    fillTestCase(numericType.getName(), value, testItems, false, ctx);
                    continue;
                }

                if (EXACT_TYPES.contains(numericType)) {
                    if (!exactNumeric.matcher(value).matches()) {
                        acceptable = false;
                    }
                } else if (numericType == FLOAT || numericType == REAL) {
                    toCompare = Float.parseFloat(value);
                } else {
                    toCompare = Double.parseDouble(value);
                }

                fillTestCase(numericType.getName(), value, testItems, acceptable, ctx, toCompare);
            }
        }

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> decimalDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        fillTestCase("DECIMAL", "100", testItems, true, ctx, new BigDecimal(100));
        fillTestCase("DECIMAL", "100.5", testItems, true, ctx, new BigDecimal(101));

        fillTestCase("DECIMAL(4, 1)", "100", testItems, true, ctx, new BigDecimal("100.0"));
        fillTestCase("DECIMAL(4, 1)", "100.4", testItems, true, ctx, new BigDecimal("100.4"));
        fillTestCase("DECIMAL(4, 1)", "100.6", testItems, true, ctx, new BigDecimal("100.6"));
        fillTestCase("DECIMAL(4, 1)", "100.12", testItems, true, ctx, new BigDecimal("100.1"));
        fillTestCase("DECIMAL(4, 1)", "1000.12", testItems, false, ctx);

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> numericTypesWithNonNumericDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        String[] values = {"'01:01:02'", "'2020-01-02 01:01:01'", "'2020-01-02'", "true", "'true'", "x'01'", "INTERVAL '1' DAY"};

        for (String value : values) {
            for (SqlTypeName numericType : NUMERIC_TYPES) {
                fillTestCase(numericType.getName(), value, testItems, false, ctx);
            }
        }

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> testCharTypesWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        fillTestCase("CHAR", "1", testItems, true, ctx, "1");
        fillTestCase("CHAR", "'1'", testItems, true, ctx, "1");
        fillTestCase("CHAR(2)", "12", testItems, true, ctx, "12");
        fillTestCase("CHAR", "12", testItems, false, ctx);
        fillTestCase("VARCHAR", "12", testItems, true, ctx, "12");
        fillTestCase("VARCHAR", "'12'", testItems, true, ctx, "12");
        fillTestCase("VARCHAR(2)", "123", testItems, false, ctx);
        fillTestCase("VARCHAR(2)", "'123'", testItems, false, ctx);

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> timestampWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        fillTestCase("TIMESTAMP", "'2020-01-02 01:01:01.23'", testItems, true, ctx,
                LocalDateTime.of(2020, 1, 2, 1, 1, 1, 230_000_000));
        fillTestCase("TIMESTAMP", "'2020-01-02'", testItems, true, ctx,
                LocalDateTime.of(2020, 1, 2, 0, 0));
        fillTestCase("TIMESTAMP", "'01:01:02'", testItems, false, ctx);
        fillTestCase("TIMESTAMP", "'1'", testItems, false, ctx);
        fillTestCase("TIMESTAMP", "1", testItems, false, ctx);
        fillTestCase("TIMESTAMP", "'2020-01-02 01:01:01ERR'", testItems, false, ctx);

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> dateWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        fillTestCase("DATE", "'2020-01-02 01:01:01'", testItems, true, ctx,
                LocalDate.of(2020, 1, 2));
        fillTestCase("DATE", "'2020-01-02'", testItems, true, ctx,
                LocalDate.of(2020, 1, 2));
        fillTestCase("DATE", "'01:01:01'", testItems, false, ctx);
        fillTestCase("DATE", "'1'", testItems, false, ctx);
        fillTestCase("DATE", "1", testItems, false, ctx);
        fillTestCase("DATE", "'2020-01-02ERR'", testItems, false, ctx);

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> timeWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        fillTestCase("TIME", "'2020-01-02 01:01:01'", testItems, true, ctx,
                LocalTime.of(1, 1, 1));
        fillTestCase("TIME", "'2020-01-02'", testItems, false, ctx);
        fillTestCase("TIME", "'01:01:01.2'", testItems, true, ctx,
                LocalTime.of(1, 1, 1, 200000000));
        fillTestCase("TIME", "'1'", testItems, false, ctx);
        fillTestCase("TIME", "1", testItems, false, ctx);
        fillTestCase("TIME", "'01:01:01ERR'", testItems, false, ctx);

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> binaryWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        fillTestCase("BINARY", "x'01'", testItems, true, ctx, fromInternal(new byte[]{(byte) 1}, byte[].class));
        fillTestCase("BINARY", "'01'", testItems, false, ctx);
        fillTestCase("BINARY", "1", testItems, false, ctx);
        fillTestCase("BINARY", "x'0102'", testItems, false, ctx);
        fillTestCase("BINARY(2)", "x'0102'", testItems, true, ctx, fromInternal(new byte[]{(byte) 1, (byte) 2}, byte[].class));
        fillTestCase("VARBINARY", "x'0102'", testItems, true, ctx, fromInternal(new byte[]{(byte) 1, (byte) 2}, byte[].class));
        fillTestCase("VARBINARY", "'0102'", testItems, false, ctx);
        fillTestCase("VARBINARY", "1", testItems, false, ctx);

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> booleanWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        fillTestCase("BOOLEAN", "true", testItems, true, ctx, true);
        fillTestCase("BOOLEAN", "'true'", testItems, false, ctx);
        fillTestCase("BOOLEAN", "'1'", testItems, false, ctx);
        fillTestCase("BOOLEAN", "'yes'", testItems, false, ctx);

        fillTestCase("BOOLEAN", "false", testItems, true, ctx);
        fillTestCase("BOOLEAN", "'false'", testItems, false, ctx);
        fillTestCase("BOOLEAN", "'0'", testItems, false, ctx);
        fillTestCase("BOOLEAN", "'no'", testItems, false, ctx);

        fillTestCase("BOOLEAN", "'2'", testItems, false, ctx);

        return testItems.stream();
    }

    @Disabled("Remove after https://issues.apache.org/jira/browse/IGNITE-17376 is implemented.")
    @TestFactory
    public Stream<DynamicTest> timestampWithTzWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();
        String template = "CREATE TABLE t (id INTEGER PRIMARY KEY, d {} DEFAULT {})";

        {
            String sql = format(template, "TIMESTAMP WITH LOCAL TIME ZONE", "'2020-01-02 01:01:01'");

            testItems.add(DynamicTest.dynamicTest(String.format("ALLOW: %s", sql), () ->
                    converter.convert((SqlDdl) parse(sql), ctx)));
        }

        return testItems.stream();
    }

    @Test
    public void tableWithAutogenPkColumn() throws SqlParseException {
        var node = parse("CREATE TABLE t (id varchar default gen_random_uuid primary key, val int) WITH STORAGE_PROFILE='"
                + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertThat(
                createTable.columns(),
                allOf(
                        hasItem(columnThat("column with name \"VAL\"", cd -> "VAL".equals(cd.name()))),
                        hasItem(columnThat("PK with functional default",
                                cd -> "ID".equals(cd.name())
                                        && !cd.nullable()
                                        && SqlTypeName.VARCHAR.equals(cd.type().getSqlTypeName())
                                        && cd.defaultValueDefinition().type() == Type.FUNCTION_CALL
                                        && "GEN_RANDOM_UUID".equals(((FunctionCall) cd.defaultValueDefinition()).functionName())
                                )
                        )
                )
        );
    }

    @Test
    public void tableWithoutStorageProfileShouldThrowError() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) with storage_profile=''");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("String cannot be empty"));

        var newNode = parse("CREATE TABLE t (val int) WITH PRIMARY_ZONE='ZONE', storage_profile=''");

        assertThat(node, instanceOf(SqlDdl.class));

        ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) newNode, createContext())
        );

        assertThat(ex.getMessage(), containsString("String cannot be empty"));
    }

    private static Matcher<ColumnDefinition> columnThat(String description, Function<ColumnDefinition, Boolean> checker) {
        return new CustomMatcher<>(description) {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof ColumnDefinition && checker.apply((ColumnDefinition) actual) == Boolean.TRUE;
            }
        };
    }

    // Transforms INTERVAL_YEAR_MONTH -> INTERVAL YEAR
    private static String makeUsableIntervalType(String typeName) {
        if (typeName.lastIndexOf('_') != typeName.indexOf('_')) {
            typeName = typeName.substring(0, typeName.lastIndexOf('_'));
        }
        typeName = typeName.replace("_", " ");
        return typeName;
    }

    // Transforms INTERVAL_YEAR_MONTH -> INTERVAL '1' YEAR
    private static String makeUsableIntervalValue(String typeName) {
        return makeUsableIntervalType(typeName).replace(" ", " '1' ");
    }

    private void fillTestCase(String type, String val, List<DynamicTest> testItems, boolean acceptable, PlanningContext ctx) {
        fillTestCase(type, val, testItems, acceptable, ctx, null);
    }

    @SuppressWarnings({"ThrowableNotThrown"})
    private void fillTestCase(String type, String val, List<DynamicTest> testItems, boolean acceptable, PlanningContext ctx,
            @Nullable Object compare) {
        String template = "CREATE TABLE t (id INTEGER PRIMARY KEY, d {} DEFAULT {})";
        String sql = format(template, type, val);

        if (acceptable) {
            testItems.add(DynamicTest.dynamicTest(String.format("ALLOW: %s", sql), () -> {
                CreateTableCommand cmd = (CreateTableCommand) converter.convert((SqlDdl) parse(sql), ctx);
                ColumnDefinition def = cmd.columns().get(1);
                DefaultValueDefinition.ConstantValue defVal = def.defaultValueDefinition();
                Object defaultValue = defVal.value();
                if (compare != null) {
                    if (compare instanceof byte[]) {
                        assertArrayEquals((byte[]) compare, (byte[]) defaultValue);
                    } else {
                        assertEquals(compare, defaultValue);
                    }
                }
            }));
        } else {
            testItems.add(DynamicTest.dynamicTest(String.format("NOT ALLOW: %s", sql), () ->
                    assertThrowsSqlException(STMT_VALIDATION_ERR, "Invalid default value for column", () ->
                            converter.convert((SqlDdl) parse(sql), ctx))));
        }
    }

    /**
     * Checks that there are no ID duplicates.
     *
     * @param set0 Set of string identifiers.
     * @param set1 Set of string identifiers.
     * @throws IllegalStateException If there is a duplicate ID.
     */
    static void checkDuplicates(Set<String> set0, Set<String> set1) {
        for (String id : set1) {
            if (set0.contains(id)) {
                throw new IllegalStateException("Duplicate id: " + id);
            }
        }
    }
}
