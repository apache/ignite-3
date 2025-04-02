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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.OPTIONS_MAPPING;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.DropZoneEntry;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.SetDefaultZoneEntry;
import org.apache.ignite.internal.partitiondistribution.DistributionAlgorithm;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Tests the conversion of a sql zone definition to a command.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DistributionZoneSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {

    private static final List<ZoneOptionEnum> NUMERIC_OPTIONS = List.of(
            ZoneOptionEnum.PARTITIONS,
            ZoneOptionEnum.REPLICAS,
            ZoneOptionEnum.DATA_NODES_AUTO_ADJUST,
            ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP,
            ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN);

    private static final List<ZoneOptionEnum> STRING_OPTIONS = List.of(
            ZoneOptionEnum.DISTRIBUTION_ALGORITHM,
            ZoneOptionEnum.DATA_NODES_FILTER,
            ZoneOptionEnum.CONSISTENCY_MODE
    );

    @BeforeAll
    public static void setUp() {
        assertThat(OPTIONS_MAPPING.size(), is(NUMERIC_OPTIONS.size() + STRING_OPTIONS.size()));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZone(boolean withPresent) throws SqlParseException {
        SqlNode node = parse(withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'"
                : "CREATE ZONE test STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.name(), equalTo("TEST"));

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.STRONG_CONSISTENCY));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithConsistencyModeStrongConsistency(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test WITH storage_profiles='" + DEFAULT_STORAGE_PROFILE + "',"
                        + " CONSISTENCY_MODE='" + ConsistencyMode.STRONG_CONSISTENCY.name() + "'"
                : "CREATE ZONE test (CONSISTENCY MODE '" + ConsistencyMode.STRONG_CONSISTENCY.name() + "') "
                        + "STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "'] ";

        SqlNode node = parse(sql);

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.STRONG_CONSISTENCY));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithConsistencyModeHighAvailability(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                        + " CONSISTENCY_MODE='" + ConsistencyMode.HIGH_AVAILABILITY + "'"
                : "CREATE ZONE test (CONSISTENCY MODE '" + ConsistencyMode.HIGH_AVAILABILITY + "') "
                        + "STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "'] ";

        SqlNode node = parse(sql);

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.HIGH_AVAILABILITY));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithConsistencyModeInvalid(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                        + " CONSISTENCY_MODE='MY_CUSTOM_MODE'"
                : "CREATE ZONE test (CONSISTENCY MODE 'MY_CUSTOM_MODE') "
                        + "STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "'] ";

        SqlNode node = parse(sql);

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrows(
                SqlException.class,
                () -> converter.convert((SqlDdl) node, createContext()),
                "Failed to parse consistency mode: MY_CUSTOM_MODE. Valid values are: [STRONG_CONSISTENCY, HIGH_AVAILABILITY]"
        );
    }

    @Test
    public void testMixedOptions() {
        String passed = "CREATE ZONE test with partitions=2, replicas=3, storage_profiles='p' S";
        assertThrowsWithPos("CREATE ZONE test with partitions=2, replicas=3, storage_profiles='p' STORAGE PROFILES ['profile']",
                "STORAGE", passed.length());

        passed = "CREATE ZONE test with (";
        assertThrowsWithPos("CREATE ZONE test with (partitions 2, replicas 3, storage_profiles='p') STORAGE PROFILES ['profile']",
                "(", passed.length());
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithOptions(boolean withPresent) throws SqlParseException {
        // Check non-conflicting options.
        {
            String sql = withPresent
                    ? "CREATE ZONE test with "
                            + "partitions=2, "
                            + "replicas=3, "
                            + "distribution_algorithm='rendezvous', "
                            + "data_nodes_filter='$[?(@.region == \"US\")]', "
                            + "data_nodes_auto_adjust=300, "
                            + "storage_profiles='lru_rocks , segmented_aipersist ' "
                    : "CREATE ZONE test "
                            + "(partitions 2, "
                            + "replicas 3, "
                            + "distribution algorithm 'rendezvous', "
                            + "nodes filter '$[?(@.region == \"US\")]', "
                            + "auto adjust 300) "
                            + "storage profiles ['lru_rocks', 'segmented_aipersist '] ";

            SqlNode node = parse(sql);

            assertThat(node, instanceOf(SqlDdl.class));

            CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

            assertThat(desc.partitions(), equalTo(2));
            assertThat(desc.replicas(), equalTo(3));
            // TODO https://issues.apache.org/jira/browse/IGNITE-22162
            // assertThat(desc.distributionAlgorithm(), equalTo("rendezvous"));
            assertThat(desc.filter(), equalTo("$[?(@.region == \"US\")]"));
            assertThat(desc.dataNodesAutoAdjust(), equalTo(300));

            List<CatalogStorageProfileDescriptor> storageProfiles = desc.storageProfiles().profiles();
            assertThat(storageProfiles, hasSize(2));
            assertThat(storageProfiles.get(0).storageProfile(), equalTo("lru_rocks"));
            assertThat(storageProfiles.get(1).storageProfile(), equalTo("segmented_aipersist"));
        }

        // Check remaining options.
        {
            String sql = withPresent
                    ? "CREATE ZONE test with "
                            + "data_nodes_auto_adjust_scale_up=100, "
                            + "data_nodes_auto_adjust_scale_down=200, "
                            + "storage_profiles='lru_rocks'"
                    : "CREATE ZONE test "
                            + "(auto scale up 100, "
                            + "auto scale down 200) "
                            + "storage profiles ['lru_rocks']";

            SqlNode node = parse(sql);

            assertThat(node, instanceOf(SqlDdl.class));

            CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

            assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(100));
            assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(200));
        }

        // Check option validation.
        if (withPresent) {
            expectOptionValidationError("CREATE ZONE test with partitions=-1, storage_profiles='p'", "PARTITION");
            expectOptionValidationError("CREATE ZONE test with replicas=-1, storage_profiles='p'", "REPLICAS");
            assertThrowsWithPos("CREATE ZONE test with replicas=FALL, storage_profiles='p'", "FALL", 32);
            emptyProfilesValidationError("CREATE ZONE test with storage_profiles='' ");
            emptyProfilesValidationError("CREATE ZONE test with storage_profiles=' ' ");
        } else {
            assertThrowsWithPos("CREATE ZONE test (partitions -1)", "-", 30);
            assertThrowsWithPos("CREATE ZONE test (replicas -1)", "-", 28);
            assertThrowsWithPos("CREATE ZONE test (replicas FALL)", "FALL", 28);
            assertThrowsWithPos("CREATE ZONE test (replicas 1, partitions -1)", "-", 42);
            assertThrowsWithPos("CREATE ZONE test storage_profiles ['']", "storage_profiles", 18);
            assertThrowsParseException("CREATE ZONE test (distribution algorithm '')", "Validation Error: Empty character literal "
                    + "is not allowed in this context.");
            assertThrowsParseException("CREATE ZONE test storage profiles ['']", "Validation Error: Empty character literal "
                    + "is not allowed in this context.");
            assertThrowsParseException("CREATE ZONE test storage profiles [' ']", "Validation Error: Empty character literal "
                    + "is not allowed in this context.");
        }
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithReplicasAll(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "', REPLICAS=ALL"
                : "CREATE ZONE test (REPLICAS ALL) STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        SqlNode node = parse(sql);

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.replicas(), equalTo(DistributionAlgorithm.ALL_REPLICAS));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithoutStorageProfileOptionShouldThrowError(boolean withPresent) throws SqlParseException {
        assertThrowsWithPos("CREATE ZONE test", "<EOF>", 16);

        if (withPresent) {
            expectOptionValidationError("CREATE ZONE test with replicas=1", "STORAGE_PROFILES");
        } else {
            assertThrowsWithPos("CREATE ZONE test (replicas 1)", "<EOF>", 29);
        }
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithDuplicateOptions(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test with partitions=2, replicas=0, PARTITIONS=1, STORAGE_PROFILES='profile'"
                : "CREATE ZONE test (partitions 2, replicas 0, PARTITIONS 1) STORAGE PROFILES ['profile']";

        SqlNode node = parse(sql);

        assertThat(node, instanceOf(SqlDdl.class));

        expectDuplicateOptionError((SqlDdl) node, "PARTITIONS");
    }

    @Test
    public void testRenameZoneCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test RENAME TO test2");

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, instanceOf(RenameZoneCommand.class));

        Mockito.when(catalog.zone("TEST")).thenReturn(mock(CatalogZoneDescriptor.class));

        AlterZoneEntry entry = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class);

        assertThat(entry.descriptor().name(), equalTo("TEST2"));
        assertThat(((RenameZoneCommand) cmd).ifExists(), is(false));
    }

    @Test
    public void testRenameZoneIfExistCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE IF EXISTS test RENAME TO test2");

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, instanceOf(RenameZoneCommand.class));

        RenameZoneCommand zoneCmd = (RenameZoneCommand) cmd;

        Mockito.when(catalog.zone("TEST")).thenReturn(mock(CatalogZoneDescriptor.class));

        AlterZoneEntry entry = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class);

        assertThat(entry.descriptor().name(), equalTo("TEST2"));
        assertThat(zoneCmd.ifExists(), is(true));
    }

    @Test
    public void testAlterZoneCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET replicas=3");

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, instanceOf(AlterZoneCommand.class));

        CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);

        Mockito.when(zoneMock.name()).thenReturn("TEST");
        Mockito.when(zoneMock.filter()).thenReturn("");

        Mockito.when(catalog.zone("TEST")).thenReturn(zoneMock);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

        assertThat(desc.name(), equalTo("TEST"));
        assertThat(desc.replicas(), is(3));
        assertThat(((AlterZoneCommand) cmd).ifExists(), is(false));
    }

    @Test
    public void testAlterZoneIfExistsCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE IF EXISTS test SET replicas=3");

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, instanceOf(AlterZoneCommand.class));
        assertThat(((AlterZoneCommand) cmd).ifExists(), is(true));
    }

    @Test
    public void testAlterZoneSetCommand() throws SqlParseException {
        // Check non-conflicting options.
        {
            SqlNode node = parse("ALTER ZONE test SET "
                    + "replicas=3, "
                    + "data_nodes_filter='$[?(@.region == \"US\")]', "
                    + "data_nodes_auto_adjust=300");

            CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
            assertThat(cmd, instanceOf(AlterZoneCommand.class));

            CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
            Mockito.when(zoneMock.name()).thenReturn("TEST");
            Mockito.when(zoneMock.filter()).thenReturn("");

            Mockito.when(catalog.zone("TEST")).thenReturn(zoneMock);

            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

            assertThat(desc.name(), equalTo("TEST"));

            assertThat(desc.replicas(), equalTo(3));
            assertThat(desc.filter(), equalTo("$[?(@.region == \"US\")]"));
            assertThat(desc.dataNodesAutoAdjust(), equalTo(300));
        }

        // Check remaining options.
        {
            SqlNode node = parse("ALTER ZONE test SET "
                    + "data_nodes_auto_adjust_scale_up=100, "
                    + "data_nodes_auto_adjust_scale_down=200");

            CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
            assertThat(cmd, instanceOf(AlterZoneCommand.class));

            CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
            Mockito.when(zoneMock.name()).thenReturn("TEST");
            Mockito.when(zoneMock.filter()).thenReturn("");

            Mockito.when(catalog.zone("TEST")).thenReturn(zoneMock);

            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

            assertThat(desc.name(), equalTo("TEST"));

            assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(100));
            assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(200));
        }
    }

    @Test
    public void testAlterZoneReplicasAll() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET replicas=ALL");

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, instanceOf(AlterZoneCommand.class));

        CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
        Mockito.when(zoneMock.name()).thenReturn("TEST");
        Mockito.when(zoneMock.filter()).thenReturn("");

        Mockito.when(catalog.zone("TEST")).thenReturn(zoneMock);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

        assertThat(desc.replicas(), is(DistributionAlgorithm.ALL_REPLICAS));
    }

    @Test
    public void testAlterZoneSetDefault() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET DEFAULT");

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, instanceOf(AlterZoneSetDefaultCommand.class));

        CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
        Mockito.when(catalog.zone("TEST")).thenReturn(zoneMock);

        SetDefaultZoneEntry entry = invokeAndGetFirstEntry(cmd, SetDefaultZoneEntry.class);

        AlterZoneSetDefaultCommand zoneCmd = (AlterZoneSetDefaultCommand) cmd;
        assertThat(entry.zoneId(), is(zoneMock.id()));
        assertThat(zoneCmd.ifExists(), is(false));
    }

    @Test
    public void testAlterZoneSetDefaultIfExists() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE IF EXISTS test SET DEFAULT");

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, instanceOf(AlterZoneSetDefaultCommand.class));

        assertThat(((AlterZoneSetDefaultCommand) cmd).ifExists(), is(true));
    }

    @Test
    public void testAlterZoneCommandWithInvalidOptions() throws SqlParseException {
        expectOptionValidationError("ALTER ZONE test SET replicas=2, data_nodes_auto_adjust=-100", "DATA_NODES_AUTO_ADJUST");
    }

    @Test
    public void testAlterZoneCommandWithDuplicateOptions() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET replicas=2, data_nodes_auto_adjust=300, DATA_NODES_AUTO_ADJUST=400");

        assertThat(node, instanceOf(SqlDdl.class));

        expectDuplicateOptionError((SqlDdl) node, "DATA_NODES_AUTO_ADJUST");
    }

    @Test
    public void testDropZone() throws SqlParseException {
        SqlNode node = parse("DROP ZONE test");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, instanceOf(DropZoneCommand.class));

        CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
        Mockito.when(catalog.zone("TEST")).thenReturn(zoneMock);

        DropZoneEntry entry = invokeAndGetFirstEntry(cmd, DropZoneEntry.class);

        assertThat(entry.zoneId(), is(zoneMock.id()));
    }

    @ParameterizedTest(name = "with syntax = {0}, option = {1}")
    @MethodSource("numericOptions")
    public void createZoneWithInvalidNumericOptionValue(boolean withPresent, ZoneOptionEnum optionName) throws Exception {
        String sql = withPresent ? "create zone test_zone with {}={}, storage_profiles='p'" : "create zone test_zone ({} {})";

        if (withPresent) {
            SqlDdl node = (SqlDdl) parse(format(sql, optionName, "'bar'"));
            expectInvalidOptionType(node, optionName.name());
        } else {
            String option = OPTIONS_MAPPING.get(optionName);
            String prefix = "create zone test_zone (";
            assertThrowsWithPos(format(sql, option, "'bar'"), "\\'bar\\'", prefix.length() + option.length() + 1 /* start pos*/
                    + 1 /* first symbol after bracket*/);

            assertThrowsWithPos(format(sql, option, "-1"), "-", prefix.length() + option.length() + 1 /* start pos*/
                    + 1 /* first symbol after bracket*/);
        }
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void createZoneWithUnexpectedOption(boolean withPresent) throws SqlParseException {
        String sql = withPresent ? "create zone test_zone with ABC=1, storage_profiles='p'" : "create zone test_zone (ABC 1)";

        if (withPresent) {
            SqlDdl node = (SqlDdl) parse(sql);
            expectUnexpectedOption(node, "ABC");
        } else {
            assertThrowsWithPos(sql, "ABC", 24);
        }
    }

    @ParameterizedTest(name = "with syntax = {0}, option = {1}")
    @MethodSource("stringOptions")
    public void createZoneWithInvalidStringOptionValue(boolean withPresent, ZoneOptionEnum optionParam) throws Exception {
        if (withPresent) {
            SqlDdl node = (SqlDdl) parse(format("create zone test_zone with {}={}, storage_profiles='p'", optionParam.name(), "1"));
            expectInvalidOptionType(node, optionParam.name());
        } else {
            String sql = "create zone test_zone ({} {})";

            String option = OPTIONS_MAPPING.get(optionParam);
            String prefix = "create zone test_zone (";
            assertThrowsWithPos(format(sql, option, "1"), "1", prefix.length() + option.length() + 1 /* start pos*/
                    + 1 /* first symbol after bracket*/);
        }
    }

    @Test
    public void createZoneWithInvalidStorageProfiles() {
        String profiles = "STORAGE PROFILES [";
        String sql = "create zone test_zone {} {}]";

        String prefix = "create zone test_zone ";

        assertThrowsWithPos(format(sql, profiles, "1"), "1", prefix.length() + profiles.length() + 1 /* start pos*/
                + 1 /* first symbol after bracket*/);
    }

    @ParameterizedTest
    @MethodSource("pureNumericOptions")
    public void alterZoneWithInvalidNumericOptionValue(ZoneOptionEnum optionParam) throws Exception {
        SqlDdl node = (SqlDdl) parse(format("alter zone test_zone set {}={}", optionParam.name(), "'bar'"));
        expectInvalidOptionType(node, optionParam.name());
    }

    @Test
    public void alterZoneWithUnexpectedOption() throws SqlParseException {
        SqlDdl node = (SqlDdl) parse("alter zone test_zone set ABC=1");
        expectUnexpectedOption(node, "ABC");
    }

    private void convert(String query) throws SqlParseException {
        SqlDdl node = (SqlDdl) parse(query);
        converter.convert(node, createContext());
    }

    private void assertThrowsWithPos(String query, String encountered, int pos) {
        assertThrowsParseException(query, format("Encountered \"{}\" at line 1, column {}.", encountered, pos));
    }

    private void assertThrowsParseException(String query, String message) {
        assertThrows(SqlParseException.class, () -> convert(query), message);
    }

    private static Stream<ZoneOptionEnum> pureNumericOptions() {
        return NUMERIC_OPTIONS.stream();
    }

    private static Stream<Arguments> numericOptions() {
        return Stream.of(true, false).flatMap(t1 -> NUMERIC_OPTIONS.stream()
                .map(t2 -> Arguments.of(t1, t2))
        );
    }

    private static Stream<Arguments> stringOptions() {
        return Stream.of(true, false).flatMap(t1 -> STRING_OPTIONS.stream()
                .map(t2 -> Arguments.of(t1, t2))
        );
    }

    private void expectOptionValidationError(String sql, String invalidOption) throws SqlParseException {
        SqlDdl node = (SqlDdl) parse(sql);
        assertThrowsWithCode(
                SqlException.class,
                Sql.STMT_VALIDATION_ERR,
                () -> converter.convert(node, createContext()),
                "Zone option validation failed [option=" + invalidOption
        );
    }

    private void emptyProfilesValidationError(String sql) throws SqlParseException {
        SqlDdl node = (SqlDdl) parse(sql);
        assertThrowsWithCode(
                SqlException.class,
                Sql.STMT_VALIDATION_ERR,
                () -> converter.convert(node, createContext()),
                "STORAGE PROFILES can not be empty"
        );
    }

    private void expectInvalidOptionType(SqlDdl node, String invalidOption) {
        assertThrowsWithCode(
                SqlException.class,
                Sql.STMT_VALIDATION_ERR,
                () -> converter.convert(node, createContext()),
                "Invalid zone option type [option=" + invalidOption
        );
    }

    private void expectUnexpectedOption(SqlDdl node, String invalidOption) {
        assertThrowsWithCode(
                SqlException.class,
                Sql.STMT_VALIDATION_ERR,
                () -> converter.convert(node, createContext()),
                "Unexpected zone option [option=" + invalidOption
        );
    }

    private void expectDuplicateOptionError(SqlDdl node, String option) {
        assertThrowsWithCode(
                SqlException.class,
                Sql.STMT_VALIDATION_ERR,
                () -> converter.convert(node, createContext()),
                "Duplicate zone option has been specified [option=" + option
        );
    }
}
