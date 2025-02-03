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
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.STORAGE_PROFILES;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
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
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Tests the conversion of a sql zone definition to a command.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DistributionZoneSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {

    private static final List<String> NUMERIC_OPTIONS = Arrays.asList("PARTITIONS", "REPLICAS", "DATA_NODES_AUTO_ADJUST",
            "DATA_NODES_AUTO_ADJUST_SCALE_UP", "DATA_NODES_AUTO_ADJUST_SCALE_DOWN");

    private static final List<String> STRING_OPTIONS = Arrays.asList(
            "DISTRIBUTION_ALGORITHM",
            "DATA_NODES_FILTER",
            "STORAGE_PROFILES"
    );

    @Test
    public void testCreateZone() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        NewZoneEntry newZoneEntry = invokeAndGetFirstEntry(cmd, NewZoneEntry.class);

        assertThat(newZoneEntry.descriptor().name(), equalTo("TEST"));
    }

    @Test
    public void testCreateZoneWithConsistencyModeNotSet() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test0 WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.STRONG_CONSISTENCY));
    }

    @Test
    public void testCreateZoneWithConsistencyModeStrongConsistency() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test0 WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                + " CONSISTENCY_MODE='" + ConsistencyMode.STRONG_CONSISTENCY.name() + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.STRONG_CONSISTENCY));
    }

    @Test
    public void testCreateZoneWithConsistencyModeHighAvailability() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test0 WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                + " CONSISTENCY_MODE='" + ConsistencyMode.HIGH_AVAILABILITY.name() + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.HIGH_AVAILABILITY));
    }

    @Test
    public void testCreateZoneWithConsistencyModeInvalid() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test0 WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                + " CONSISTENCY_MODE='" + "MY_CUSTOM_MODE" + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrows(
                SqlException.class,
                () -> converter.convert((SqlDdl) node, createContext()),
                "Failed to parse consistency mode: MY_CUSTOM_MODE. Valid values are: [STRONG_CONSISTENCY, HIGH_AVAILABILITY]"
        );
    }

    @Test
    public void testCreateZoneWithOptions() throws SqlParseException {
        // Check non-conflicting options.
        {
            SqlNode node = parse("CREATE ZONE test with "
                    + "partitions=2, "
                    + "replicas=3, "
                    + "distribution_algorithm='rendezvous', "
                    + "data_nodes_filter='$[?(@.region == \"US\")]', "
                    + "data_nodes_auto_adjust=300, "
                    + "storage_profiles='lru_rocks, segmented_aipersist' "
            );

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
            SqlNode node = parse("CREATE ZONE test with "
                    + "data_nodes_auto_adjust_scale_up=100, "
                    + "data_nodes_auto_adjust_scale_down=200, "
                    + "storage_profiles='lru_rocks'");

            assertThat(node, instanceOf(SqlDdl.class));

            CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());
            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

            assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(100));
            assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(200));
        }

        // Check option validation.
        expectOptionValidationError("CREATE ZONE test with partitions=-1", "PARTITION");
        expectOptionValidationError("CREATE ZONE test with replicas=-1", "REPLICAS");
        expectOptionValidationError("CREATE ZONE test with storage_profiles='' ", "STORAGE_PROFILES");
    }

    @Test
    public void testCreateZoneWithReplicasAll() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                + " REPLICAS=ALL");

        assertThat(node, instanceOf(SqlDdl.class));

        CatalogCommand cmd = converter.convert((SqlDdl) node, createContext());

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.replicas(), equalTo(Integer.MAX_VALUE));
    }

    @Test
    public void testCreateZoneWithoutStorageProfileOptionShouldThrowError() throws SqlParseException {
        SqlNode node =  parse("CREATE ZONE test");

        assertThat(node, instanceOf(SqlDdl.class));

        assertThrows(
                SqlException.class,
                () -> converter.convert((SqlDdl) node, createContext()),
                STORAGE_PROFILES + " option cannot be null"
        );

        SqlNode newNode = parse("CREATE ZONE test with replicas=1");

        assertThat(newNode, instanceOf(SqlDdl.class));

        assertThrows(
                SqlException.class,
                () -> converter.convert((SqlDdl) newNode, createContext()),
                STORAGE_PROFILES + " option cannot be null"
        );
    }

    @Test
    public void testCreateZoneWithDuplicateOptions() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test with partitions=2, replicas=0, PARTITIONS=1");

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

        assertThat(desc.replicas(), is(Integer.MAX_VALUE));
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

    @ParameterizedTest
    @MethodSource("numericOptions")
    public void createZoneWithInvalidNumericOptionValue(String optionName) throws Exception {
        SqlDdl node = (SqlDdl) parse(format("create zone test_zone with {}={}", optionName, "'bar'"));
        expectInvalidOptionType(node, optionName);
    }

    @Test
    public void createZoneWithUnexpectedOption() throws SqlParseException {
        SqlDdl node = (SqlDdl) parse("create zone test_zone with ABC=1");
        expectUnexpectedOption(node, "ABC");
    }

    @ParameterizedTest
    @MethodSource("stringOptions")
    public void createZoneWithInvalidStringOptionValue(String optionName) throws Exception {
        SqlDdl node = (SqlDdl) parse(format("create zone test_zone with {}={}", optionName, "1"));
        expectInvalidOptionType(node, optionName);
    }

    @ParameterizedTest
    @MethodSource("numericOptions")
    public void alterZoneWithInvalidNumericOptionValue(String optionName) throws Exception {
        SqlDdl node = (SqlDdl) parse(format("alter zone test_zone set {}={}", optionName, "'bar'"));
        expectInvalidOptionType(node, optionName);
    }

    @Test
    public void alterZoneWithUnexpectedOption() throws SqlParseException {
        SqlDdl node = (SqlDdl) parse("alter zone test_zone set ABC=1");
        expectUnexpectedOption(node, "ABC");
    }

    @ParameterizedTest
    @ValueSource(strings = "DATA_NODES_FILTER")
    public void alterZoneWithInvalidStringOptionValue(String optionName) throws Exception {
        SqlDdl node = (SqlDdl) parse(format("alter zone test_zone set {}={}", optionName, "1"));
        expectInvalidOptionType(node, optionName);
    }

    private static Stream<String> numericOptions() {
        return NUMERIC_OPTIONS.stream();
    }

    private static Stream<String> stringOptions() {
        return STRING_OPTIONS.stream();
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
