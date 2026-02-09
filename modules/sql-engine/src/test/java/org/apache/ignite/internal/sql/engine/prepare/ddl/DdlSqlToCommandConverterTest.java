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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.EXACT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STALE_ROWS_FRACTION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseStorageProfiles;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.fromInternal;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.unimi.dsi.fastutil.ints.IntList;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.ConstantValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterTablePropertiesEntry;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.table.distributed.TableStatsStalenessConfiguration;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * For {@link DdlSqlToCommandConverter} testing.
 */
public class DdlSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {
    private static final Integer TEST_ZONE_ID = 100;

    private static final List<SqlTypeName> SIGNED_NUMERIC_TYPES = NUMERIC_TYPES.stream()
            .filter(t -> !SqlTypeName.UNSIGNED_TYPES.contains(t))
            .collect(Collectors.toList());

    @BeforeEach
    void setUp() {
        Supplier<TableStatsStalenessConfiguration> statStalenessProperties = () -> new TableStatsStalenessConfiguration(
                DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);
        converter = new DdlSqlToCommandConverter(storageProfiles -> completedFuture(null), filter -> completedFuture(null),
                statStalenessProperties);
    }

    @Test
    void testCheckDuplicates() {
        assertThrows(
                IllegalStateException.class,
                () -> checkDuplicates(
                        Set.of("replicas", "partitionDistribution"),
                        Set.of("partitions", "replicas")
                ),
                "Duplicate id: replicas"
        );

        assertDoesNotThrow(() -> checkDuplicates(
                        Set.of("replicas", "partitionDistribution"),
                        Set.of("replicas0", "partitionDistribution0")
                )
        );
    }

    @Test
    public void tableWithoutPkShouldThrowErrorWhenSysPropDefault() throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (val int) STORAGE PROFILE '" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Table without PRIMARY KEY is not supported",
                () -> convert((SqlDdl) node, createContext())
        );
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "false")
    public void tableWithoutPkShouldThrowErrorWhenSysPropDisabled() throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (val int) STORAGE PROFILE '" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Table without PRIMARY KEY is not supported",
                () -> convert((SqlDdl) node, createContext())
        );
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithoutPkShouldInjectImplicitPkWhenSysPropEnabled() throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (val int) STORAGE PROFILE '" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        mockCatalogSchemaAndZone("TEST_ZONE");

        List<UpdateEntry> entries = cmd.get(new UpdateContext(catalog));

        assertThat(entries.size(), greaterThan(1));

        CatalogTableDescriptor tblDesc = ((NewTableEntry) entries.get(0)).descriptor();

        NewIndexEntry idxEntry = (NewIndexEntry) entries.get(1);

        assertThat(
                tblDesc.columns(),
                allOf(
                        hasItem(columnThat("column with name \"VAL\"", cd -> "VAL".equals(cd.name()))),
                        hasItem(columnThat("implicit PK col", cd -> Commons.IMPLICIT_PK_COL_NAME.equals(cd.name())
                                && !cd.nullable() && ColumnType.UUID == cd.type()))
                )
        );

        assertThat(tblDesc.primaryKeyColumns(), contains(0));

        assertThat(idxEntry.descriptor().indexType(), is(CatalogIndexDescriptorType.HASH));
    }

    @ParameterizedTest
    @CsvSource({
            "ASC, ASC_NULLS_LAST",
            "DESC, DESC_NULLS_FIRST"
    })
    public void tableWithSortedPk(String sqlCol, CatalogColumnCollation collation) throws SqlParseException {
        String query = format("CREATE TABLE t (id int, val int, PRIMARY KEY USING SORTED (id {}))", sqlCol);
        SqlNode node = parse(query);

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        mockCatalogSchemaAndZone("TEST_ZONE");

        List<UpdateEntry> entries = cmd.get(new UpdateContext(catalog));

        assertThat(entries.size(), greaterThan(1));

        NewTableEntry tblEntry = (NewTableEntry) entries.get(0);
        NewIndexEntry idxEntry = (NewIndexEntry) entries.get(1);

        assertThat(idxEntry.descriptor().indexType(), is(CatalogIndexDescriptorType.SORTED));
        assertThat(tblEntry.descriptor().primaryKeyColumns(), equalTo(IntList.of(0)));
        assertThat(((CatalogSortedIndexDescriptor) idxEntry.descriptor()).columns().get(0).collation(), is(collation));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE t (c1 int PRIMARY KEY, c2 int PRIMARY KEY, c3 int)",
            "CREATE TABLE t (c1 int, c2 int, c3 int, PRIMARY KEY (c1), PRIMARY KEY (c1) )",
            "CREATE TABLE t (c1 int, c2 int, c3 int, PRIMARY KEY (c1), PRIMARY KEY (c2) )",
    })
    public void tablePkAppearsOnlyOnce(String stmt) throws SqlParseException {
        SqlNode node = parse(stmt);
        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(STMT_VALIDATION_ERR,
                "Unexpected number of primary key constraints [expected at most one, but was 2",
                () -> convert((SqlDdl) node, createContext())
        );
    }

    @Test
    public void tableWithHashPk() throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (id int, val int, PRIMARY KEY USING HASH (id))");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        mockCatalogSchemaAndZone("TEST_ZONE");

        List<UpdateEntry> entries = cmd.get(new UpdateContext(catalog));

        assertThat(entries.size(), greaterThan(1));

        NewTableEntry tblEntry = (NewTableEntry) entries.get(0);
        NewIndexEntry idxEntry = (NewIndexEntry) entries.get(1);

        assertThat(idxEntry.descriptor().indexType(), is(CatalogIndexDescriptorType.HASH));
        assertThat(idxEntry.descriptor(), Matchers.instanceOf(CatalogHashIndexDescriptor.class));
        assertThat(tblEntry.descriptor().primaryKeyColumns(), equalTo(IntList.of(0)));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithIdentifierZone() throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (id int) ZONE test_zone");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        mockCatalogSchemaAndZone("TEST_ZONE");

        NewTableEntry tblEntry = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(tblEntry.descriptor().zoneId(), is(TEST_ZONE_ID));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithLiteralZone() throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (id int) ZONE \"test_zone\"");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        mockCatalogSchemaAndZone("test_zone");

        NewTableEntry tblEntry = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(tblEntry.descriptor().zoneId(), is(TEST_ZONE_ID));
    }

    @ParameterizedTest
    @CsvSource(value = {
            // Negative values are rejected by the parser
            // Char
            "VARCHAR(0); VARCHAR length 0 must be between 1 and 2147483647",
            // Binary
            "VARBINARY(0); VARBINARY length 0 must be between 1 and 2147483647",
            // Decimal
            "DECIMAL(0); DECIMAL precision 0 must be between 1 and 32767",
            "DECIMAL(100000000); DECIMAL precision 100000000 must be between 1 and 32767",
            "DECIMAL(100, 100000000); DECIMAL scale 100000000 must be between 0 and 32767",
            // Timestamp
            "TIME(100000000); TIME precision 100000000 must be between 0 and 9",
            "TIMESTAMP(100000000); TIMESTAMP precision 100000000 must be between 0 and 9",
    }, delimiter = ';')
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithIncorrectType(String type, String error) throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (val " + type + ")");

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                error + ". [column=VAL]",
                () -> convert((SqlDdl) node, createContext())
        );
    }

    @ParameterizedTest
    @CsvSource(value = {
            // Negative values are rejected by the parser
            // Char
            "VARCHAR(0); VARCHAR length 0 must be between 1 and 2147483647",
            // Binary
            "VARBINARY(0); VARBINARY length 0 must be between 1 and 2147483647",
            // Decimal
            "DECIMAL(0); DECIMAL precision 0 must be between 1 and 32767",
            "DECIMAL(100000000); DECIMAL precision 100000000 must be between 1 and 32767",
            "DECIMAL(100, 100000000); DECIMAL scale 100000000 must be between 0 and 32767",
            // Timestamp
            "TIME(100000000); TIME precision 100000000 must be between 0 and 9",
            "TIMESTAMP(100000000); TIMESTAMP precision 100000000 must be between 0 and 9",
    }, delimiter = ';')
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableAddColumnWithIncorrectType(String type, String error) throws SqlParseException {
        SqlNode node = parse("ALTER TABLE t ADD COLUMN val " + type);

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                error + ". [column=VAL]",
                () -> convert((SqlDdl) node, createContext())
        );
    }

    @TestFactory
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17373")
    public Stream<DynamicTest> numericDefaultWithIntervalTypes() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        for (SqlTypeName numType : SIGNED_NUMERIC_TYPES) {
            for (SqlTypeName intervalType : INTERVAL_TYPES) {
                RelDataType initialNumType = Commons.typeFactory().createSqlType(numType);
                Object value = SqlTestUtils.generateValueByType(initialNumType);
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
            for (SqlTypeName numType : SIGNED_NUMERIC_TYPES) {
                String value = makeUsableIntervalValue(intervalType.getName());

                fillTestCase(numType.getName(), value, testItems, false, ctx);
            }
        }

        return testItems.stream();
    }

    @TestFactory
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17373")
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

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17373")
    @TestFactory
    public Stream<DynamicTest> intervalDefaultsWithIntervalTypes() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        assertEquals(Period.of(1, 1, 0), fromInternal(13, ColumnType.PERIOD));
        assertEquals(Period.of(1, 0, 0), fromInternal(12, ColumnType.PERIOD));

        fillTestCase("INTERVAL YEARS", "INTERVAL '1' YEAR", testItems, true, ctx, fromInternal(12, ColumnType.PERIOD));
        fillTestCase("INTERVAL YEARS", "INTERVAL '12' MONTH", testItems, true, ctx, fromInternal(12, ColumnType.PERIOD));
        fillTestCase("INTERVAL YEARS TO MONTHS", "INTERVAL '1' YEAR", testItems, true, ctx, fromInternal(12, ColumnType.PERIOD));
        fillTestCase("INTERVAL YEARS TO MONTHS", "INTERVAL '13' MONTH", testItems, true, ctx, fromInternal(13, ColumnType.PERIOD));
        fillTestCase("INTERVAL MONTHS", "INTERVAL '1' YEAR", testItems, true, ctx, fromInternal(12, ColumnType.PERIOD));
        fillTestCase("INTERVAL MONTHS", "INTERVAL '13' MONTHS", testItems, true, ctx, fromInternal(13, ColumnType.PERIOD));

        long oneDayMillis = Duration.ofDays(1).toMillis();
        long oneHourMillis = Duration.ofHours(1).toMillis();
        long oneMinuteMillis = Duration.ofMinutes(1).toMillis();
        long oneSecondMillis = Duration.ofSeconds(1).toMillis();

        fillTestCase("INTERVAL DAYS", "INTERVAL '1' DAY", testItems, true, ctx, fromInternal(oneDayMillis, ColumnType.DURATION));
        fillTestCase("INTERVAL DAYS TO HOURS", "INTERVAL '1' HOURS", testItems, true, ctx,
                fromInternal(oneHourMillis, ColumnType.DURATION));
        fillTestCase("INTERVAL HOURS TO SECONDS", "INTERVAL '1' MINUTE", testItems, true, ctx,
                fromInternal(oneMinuteMillis, ColumnType.DURATION));
        fillTestCase("INTERVAL MINUTES TO SECONDS", "INTERVAL '1' MINUTE", testItems, true, ctx,
                fromInternal(oneMinuteMillis, ColumnType.DURATION));
        fillTestCase("INTERVAL MINUTES TO SECONDS", "INTERVAL '1' SECOND", testItems, true, ctx,
                fromInternal(oneSecondMillis, ColumnType.DURATION));

        return testItems.stream();
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testUuidWithDefaults() throws SqlParseException {
        PlanningContext ctx = createContext();
        String template = "CREATE TABLE t (id INTEGER PRIMARY KEY, d UUID DEFAULT {})";

        String sql = format(template, "NULL");
        CreateTableCommand cmd = (CreateTableCommand) convert((SqlDdl) parse(sql), ctx);

        mockCatalogSchemaAndZone("TEST_ZONE");
        CatalogTableDescriptor tblDesc = invokeAndGetFirstEntry(cmd, NewTableEntry.class).descriptor();

        CatalogTableColumnDescriptor colDesc = tblDesc.columns().get(1);
        ConstantValue defVal = (ConstantValue) colDesc.defaultValue();
        assertNotNull(defVal);
        assertNull(defVal.value());

        UUID uuid = UUID.randomUUID();
        sql = format(template, "'" + uuid + "'");
        cmd = (CreateTableCommand) convert((SqlDdl) parse(sql), ctx);

        tblDesc = invokeAndGetFirstEntry(cmd, NewTableEntry.class).descriptor();
        colDesc = tblDesc.columns().get(1);
        defVal = (ConstantValue) colDesc.defaultValue();
        assertNotNull(defVal);
        assertEquals(uuid, defVal.value());

        String[] values = {"'01:01:02'", "'2020-01-02 01:01:01'", "'2020-01-02'", "true", "'true'", "x'01'", "INTERVAL '1' DAY"};
        for (String value : values) {
            String sql0 = format(template, value);
            assertThrowsSqlException(STMT_VALIDATION_ERR, "Invalid default value for column", () ->
                    convert((SqlDdl) parse(sql0), ctx));
        }
    }

    @TestFactory
    public Stream<DynamicTest> numericTypesWithNumericDefaults() {
        Pattern exactNumeric = Pattern.compile("^\\d+$");
        Pattern numeric = Pattern.compile("^\\d+(\\.{1}\\d*)?$");
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

        String[] numbers = {"100.4", "100.6", "100", "'100'", "'100.1'"};

        List<SqlTypeName> typesWithoutDecimal = new ArrayList<>(SIGNED_NUMERIC_TYPES);
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
            for (SqlTypeName numericType : SIGNED_NUMERIC_TYPES) {
                fillTestCase(numericType.getName(), value, testItems, false, ctx);
            }
        }

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> testCharTypesWithDefaults() {
        List<DynamicTest> testItems = new ArrayList<>();
        PlanningContext ctx = createContext();

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

        fillTestCase("VARBINARY", "x'0102'", testItems, true, ctx, new byte[]{(byte) 1, (byte) 2});
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
                    convert((SqlDdl) parse(sql), ctx)));
        }

        return testItems.stream();
    }

    @ParameterizedTest
    @ValueSource(strings = {"rand_uuid", "rand_uuid()"})
    public void tableWithAutogenPkColumn(String func) throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (id uuid default " + func  + " primary key, val int) STORAGE PROFILE '"
                + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        mockCatalogSchemaAndZone("TEST_ZONE");

        NewTableEntry tblEntry = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(
                tblEntry.descriptor().columns(),
                allOf(
                        hasItem(columnThat("column with name \"VAL\"", cd -> "VAL".equals(cd.name()))),
                        hasItem(columnThat("PK with functional default",
                                        col -> "ID".equals(col.name())
                                                && !col.nullable()
                                                && ColumnType.UUID == col.type()
                                                && col.defaultValue().type() == DefaultValue.Type.FUNCTION_CALL
                                                && "RAND_UUID".equals(((DefaultValue.FunctionCall) col.defaultValue()).functionName())
                                )
                        )
                )
        );
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithEmptyStorageProfileShouldThrowError() throws SqlParseException {
        SqlNode node = parse("CREATE TABLE t (val int) storage profile ''");

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(STMT_VALIDATION_ERR,
                "String cannot be empty",
                () -> convert((SqlDdl) node, createContext())
        );

        SqlNode newNode = parse("CREATE TABLE t (val int) ZONE ZONE storage profile ''");

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "String cannot be empty",
                () -> convert((SqlDdl) newNode, createContext())
        );
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
    //  Remove this after interval type support is added.
    @ParameterizedTest
    @MethodSource("intervalTypeNames")
    public void testCreateTableDoNotAllowIntervalTypes(SqlTypeName sqlTypeName) throws SqlParseException {
        String typeName = intervalSqlName(sqlTypeName);
        String error = format("Type {} cannot be used in a column definition [column=P].", sqlTypeName.getSpaceName());

        {
            SqlNode node = parse(format("CREATE TABLE t (id INTEGER PRIMARY KEY, p INTERVAL {})", typeName));
            assertThat(node, instanceOf(SqlDdl.class));

            assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                    () -> convert((SqlDdl) node, createContext()));
        }

        {
            SqlNode node = parse(format("CREATE TABLE t (id INTEGER PRIMARY KEY, p INTERVAL {} NOT NULL)", typeName));
            assertThat(node, instanceOf(SqlDdl.class));

            assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                    () -> convert((SqlDdl) node, createContext()));
        }
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
    //  Remove this after interval type support is added.
    @ParameterizedTest
    @MethodSource("intervalTypeNames")
    public void testAlterTableNotAllowIntervalTypes(SqlTypeName sqlTypeName) throws SqlParseException {
        String typeName = intervalSqlName(sqlTypeName);
        String error = format("Type {} cannot be used in a column definition [column=P].", sqlTypeName.getSpaceName());

        {
            SqlNode node = parse(format("ALTER TABLE t ADD COLUMN p INTERVAL {}", typeName));
            assertThat(node, instanceOf(SqlDdl.class));

            assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                    () -> convert((SqlDdl) node, createContext()));
        }

        {
            SqlNode node = parse(format("ALTER TABLE t ADD COLUMN p INTERVAL {} NOT NULL", typeName));
            assertThat(node, instanceOf(SqlDdl.class));

            assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                    () -> convert((SqlDdl) node, createContext()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"rand_uuid", "random_uuid()"})
    public void testAlterTableAddColumnFunctionDefaultIsRejected(String func) throws SqlParseException {
        SqlNode node = parse("ALTER TABLE t ADD COLUMN a UUID DEFAULT " + func);
        assertThat(node, instanceOf(SqlDdl.class));

        assertThrows(CatalogValidationException.class,
                () -> convert((SqlDdl) node, createContext()),
                "Functional defaults are not supported for non-primary key columns [col=A]"
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"rand_uuid", "random_uuid()"})
    public void testAlterSetFunctionDefault(String func) throws SqlParseException {
        SqlNode node = parse("ALTER TABLE t ALTER COLUMN a SET DEFAULT " + func);
        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand command = convert((SqlDdl) node, createContext());
        assertInstanceOf(AlterTableAlterColumnCommand.class, command);
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "a.b.c; Unsupported default expression: A.B.C",
            "length('abcd'); Unsupported default expression: `LENGTH`('abcd')",
            "1+2; Unsupported default expression: 1 + 2",
            "(1+2); Unsupported default expression: 1 + 2"
    })
    public void testCreateTableRejectUnsupportedDefault(String defaultExpr, String error) throws SqlParseException {
        String sql = format("create table t(id int default {}, val varchar, primary key (id))", defaultExpr);
        SqlNode node = parse(sql);
        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                () -> convert((SqlDdl) node, createContext()));
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "a.b.c; Unsupported default expression: A.B.C",
            "length('abcd'); Unsupported default expression: `LENGTH`('abcd')",
            "1+2; Unsupported default expression: 1 + 2",
            "(1+2); Unsupported default expression: 1 + 2"
    })
    public void testAddColumnRejectUnsupportedDefault(String defaultExpr, String error) throws SqlParseException {
        String sql = format("alter table t add column val int default {}", defaultExpr);
        SqlNode node = parse(sql);
        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                () -> convert((SqlDdl) node, createContext()));
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "a.b.c; Unsupported default expression: A.B.C",
            "length('abcd'); Unsupported default expression: `LENGTH`('abcd')",
            "1+2; Unsupported default expression: 1 + 2",
            "(1+2); Unsupported default expression: 1 + 2"
    })
    public void testAlterColumnSetDataTypeRejectUnsupportedDefault(String defaultExpr, String error) throws SqlParseException {
        String sql = format("alter table t alter column val set data type int default {}", defaultExpr);
        SqlNode node = parse(sql);
        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                () -> convert((SqlDdl) node, createContext()));
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "a.b.c; Unsupported default expression: A.B.C",
            "length('abcd'); Unsupported default expression: `LENGTH`('abcd')",
            "1+2; Unsupported default expression: 1 + 2",
            "(1+2); Unsupported default expression: 1 + 2"
    })
    public void testAlterColumnSetDefaultRejectUnsupportedDefault(String defaultExpr, String error) throws SqlParseException {
        String sql = format("alter table t alter column val set default {}", defaultExpr);
        SqlNode node = parse(sql);
        assertThat(node, instanceOf(SqlDdl.class));

        assertThrowsSqlException(STMT_VALIDATION_ERR, error,
                () -> convert((SqlDdl) node, createContext()));
    }

    @Test
    void createTableWithNonDefaultStalenessConfig() throws SqlParseException {
        AtomicReference<Long> staleRowsCount = new AtomicReference<>(DEFAULT_MIN_STALE_ROWS_COUNT / 2);
        AtomicReference<Double> staleRowsFraction = new AtomicReference<>(DEFAULT_STALE_ROWS_FRACTION / 2);

        assert staleRowsCount.get() != 0 && staleRowsFraction.get() != 0.0d;

        Supplier<TableStatsStalenessConfiguration> statStalenessProperties =
                () -> new TableStatsStalenessConfiguration(staleRowsFraction.get(), staleRowsCount.get());

        converter = new DdlSqlToCommandConverter(storageProfiles -> completedFuture(null), filter -> completedFuture(null),
                statStalenessProperties);

        CatalogCommand cmd = convert("CREATE TABLE t (id INT PRIMARY KEY, val INT)");

        mockCatalogSchemaAndZone("TEST_ZONE");

        NewTableEntry newTable = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(newTable.descriptor().properties().minStaleRowsCount(), is(staleRowsCount.get()));
        assertThat(newTable.descriptor().properties().staleRowsFraction(), is(staleRowsFraction.get()));

        staleRowsCount.set(staleRowsCount.get() + 1);
        staleRowsFraction.set(staleRowsFraction.get() + 0.1d);

        cmd = convert("CREATE TABLE t2 (id INT PRIMARY KEY, val INT)");

        newTable = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(newTable.descriptor().properties().minStaleRowsCount(), is(staleRowsCount.get()));
        assertThat(newTable.descriptor().properties().staleRowsFraction(), is(staleRowsFraction.get()));
    }

    @Test
    void createTableWithMinStaleRows() throws SqlParseException {
        CatalogCommand cmd = convert(
                "CREATE TABLE t (id INT PRIMARY KEY, val INT) WITH (min stale rows 321)"
        );

        mockCatalogSchemaAndZone("TEST_ZONE");

        NewTableEntry newTable = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(newTable.descriptor().properties().minStaleRowsCount(), is(321L));
        assertThat(newTable.descriptor().properties().staleRowsFraction(), is(DEFAULT_STALE_ROWS_FRACTION));
    }

    @Test
    void createTableWithStaleRowsFraction() throws SqlParseException {
        CatalogCommand cmd = convert(
                "CREATE TABLE t (id INT PRIMARY KEY, val INT) WITH (stale rows fraction 0.321)"
        );

        mockCatalogSchemaAndZone("TEST_ZONE");

        NewTableEntry newTable = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(newTable.descriptor().properties().minStaleRowsCount(), is(DEFAULT_MIN_STALE_ROWS_COUNT));
        assertThat(newTable.descriptor().properties().staleRowsFraction(), is(0.321));
    }

    @Test
    void createTableWithMinStaleAndRowsFraction() throws SqlParseException {
        CatalogCommand cmd = convert(
                "CREATE TABLE t (id INT PRIMARY KEY, val INT) WITH (" 
                        + "stale rows fraction 0.321, min stale rows 321)"
        );

        mockCatalogSchemaAndZone("TEST_ZONE");

        NewTableEntry newTable = invokeAndGetFirstEntry(cmd, NewTableEntry.class);

        assertThat(newTable.descriptor().properties().minStaleRowsCount(), is(321L));
        assertThat(newTable.descriptor().properties().staleRowsFraction(), is(0.321));
    }

    @Test
    void alterTableSetMinStaleRows() throws SqlParseException {
        CatalogCommand cmd = convert(
                "ALTER TABLE public.t SET min stale rows 321"
        );

        mockCatalogSchemaAndZoneAndOptTable("TEST_ZONE", "T");

        AlterTablePropertiesEntry alterTable = invokeAndGetFirstEntry(cmd, AlterTablePropertiesEntry.class);

        assertThat(alterTable.minStaleRowsCount(), is(321L));
        assertThat(alterTable.staleRowsFraction(), nullValue());
    }

    @Test
    void alterTableSetStaleRowsFraction() throws SqlParseException {
        CatalogCommand cmd = convert(
                "ALTER TABLE t SET stale rows fraction 0.321"
        );

        mockCatalogSchemaAndZoneAndOptTable("TEST_ZONE", "T");

        AlterTablePropertiesEntry alterTable = invokeAndGetFirstEntry(cmd, AlterTablePropertiesEntry.class);

        assertThat(alterTable.minStaleRowsCount(), nullValue());
        assertThat(alterTable.staleRowsFraction(), is(0.321));
    }

    @Test
    void alterTableSetMinStaleAndRowsFraction() throws SqlParseException {
        CatalogCommand cmd = convert(
                "ALTER TABLE t SET (stale rows fraction 0.321, min stale rows 321)"
        );

        mockCatalogSchemaAndZoneAndOptTable("TEST_ZONE", "T");

        AlterTablePropertiesEntry alterTable = invokeAndGetFirstEntry(cmd, AlterTablePropertiesEntry.class);

        assertThat(alterTable.minStaleRowsCount(), is(321L));
        assertThat(alterTable.staleRowsFraction(), is(0.321));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE t (id INT PRIMARY KEY, val INT) WITH (stale rows fraction 0.321, stale rows fraction 0.321)",
            "CREATE TABLE t (id INT PRIMARY KEY, val INT) WITH (min stale rows 321, min stale rows 321)",
            "CREATE TABLE t (id INT PRIMARY KEY, val INT) WITH (" 
                    + "min stale rows 321, stale rows fraction 0.321, min stale rows 321)",

            "ALTER TABLE t SET (stale rows fraction 0.321, stale rows fraction 0.321)",
            "ALTER TABLE t SET (min stale rows 321, min stale rows 321)",
            "ALTER TABLE t SET (min stale rows 321, stale rows fraction 0.321, min stale rows 321)",
    })
    void duplicatePropertiesAreNotAllowed(String query) {
        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Duplicate table property has been specified",
                () -> convert(query)
        );
    }

    private static Set<SqlTypeName> intervalTypeNames() {
        return INTERVAL_TYPES;
    }

    private static String intervalSqlName(SqlTypeName intervalType) {
        String typeName;
        if (intervalType.getStartUnit() != intervalType.getEndUnit()) {
            typeName = intervalType.getStartUnit().name() + " TO " + intervalType.getEndUnit().name();
        } else {
            typeName = intervalType.getStartUnit().name();
        }
        return typeName;
    }

    private static Matcher<CatalogTableColumnDescriptor> columnThat(String description,
            Function<CatalogTableColumnDescriptor, Boolean> checker) {
        return new CustomMatcher<>(description) {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof CatalogTableColumnDescriptor
                        && checker.apply((CatalogTableColumnDescriptor) actual) == Boolean.TRUE;
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

    @SuppressWarnings("ThrowableNotThrown")
    private void fillTestCase(String type, String val, List<DynamicTest> testItems, boolean acceptable, PlanningContext ctx,
            @Nullable Object compare) {
        String template = "CREATE TABLE t (id INTEGER PRIMARY KEY, d {} DEFAULT {})";
        String sql = format(template, type, val);

        if (acceptable) {
            testItems.add(DynamicTest.dynamicTest(String.format("ALLOW: %s", sql), () -> {
                CreateTableCommand cmd = (CreateTableCommand) convert((SqlDdl) parse(sql), ctx);

                mockCatalogSchemaAndZone("TEST_ZONE");
                CatalogTableDescriptor tblDesc = invokeAndGetFirstEntry(cmd, NewTableEntry.class).descriptor();
                CatalogTableColumnDescriptor columnDescriptor = tblDesc.columns().get(1);

                ConstantValue defVal = (ConstantValue) columnDescriptor.defaultValue();
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
                            convert((SqlDdl) parse(sql), ctx))));
        }
    }

    private void mockCatalogSchemaAndZone(String zoneName) {
        mockCatalogSchemaAndZoneAndOptTable(zoneName, null);
    }

    private void mockCatalogSchemaAndZoneAndOptTable(String zoneName, @Nullable String tableName) {
        CatalogSchemaDescriptor schemaMock = Mockito.mock(CatalogSchemaDescriptor.class);
        CatalogZoneDescriptor zoneMock = Mockito.mock(CatalogZoneDescriptor.class);
        Mockito.when(zoneMock.storageProfiles()).thenReturn(fromParams(parseStorageProfiles("default")));
        Mockito.when(zoneMock.id()).thenReturn(TEST_ZONE_ID);
        Mockito.when(catalog.schema("PUBLIC")).thenReturn(schemaMock);
        Mockito.when(catalog.defaultZone()).thenReturn(zoneMock);
        Mockito.when(catalog.zone(zoneName)).thenReturn(zoneMock);

        if (tableName != null) {
            CatalogTableDescriptor tableMock = Mockito.mock(CatalogTableDescriptor.class);
            Mockito.when(schemaMock.table(tableName)).thenReturn(tableMock);
        }
    }

    /** Checks that there are no ID duplicates. */
    private static void checkDuplicates(Set<String> set0, Set<String> set1) {
        for (String id : set1) {
            if (set0.contains(id)) {
                throw new IllegalStateException("Duplicate id: " + id);
            }
        }
    }
}
