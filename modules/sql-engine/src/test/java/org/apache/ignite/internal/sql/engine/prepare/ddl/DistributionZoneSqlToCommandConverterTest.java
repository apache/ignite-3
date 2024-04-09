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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the conversion of a sql zone definition to a command.
 */
public class DistributionZoneSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {

    private static final List<String> NUMERIC_OPTIONS = Arrays.asList("PARTITIONS", "REPLICAS", "DATA_NODES_AUTO_ADJUST",
            "DATA_NODES_AUTO_ADJUST_SCALE_UP", "DATA_NODES_AUTO_ADJUST_SCALE_DOWN");

    private static final List<String> STRING_OPTIONS = Arrays.asList(
            "AFFINITY_FUNCTION",
            "DATA_NODES_FILTER",
            "STORAGE_PROFILES"
    );

    @Test
    public void testCreateZone() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateZoneCommand.class));

        CreateZoneCommand zoneCmd = (CreateZoneCommand) cmd;

        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
    }

    @Test
    public void testCreateZoneWithOptions() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test with "
                + "partitions=2, "
                + "replicas=3, "
                + "affinity_function='rendezvous', "
                + "data_nodes_filter='\"attr1\" && \"attr2\"', "
                + "data_nodes_auto_adjust_scale_up=100, "
                + "data_nodes_auto_adjust_scale_down=200, "
                + "data_nodes_auto_adjust=300, "
                + "storage_profiles='lru_rocks, segmented_aipersist' "
        );

        assertThat(node, instanceOf(SqlDdl.class));

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());

        CreateZoneCommand createZone = (CreateZoneCommand) cmd;

        assertThat(createZone.partitions(), equalTo(2));
        assertThat(createZone.replicas(), equalTo(3));
        assertThat(createZone.affinity(), equalTo("rendezvous"));
        assertThat(createZone.nodeFilter(), equalTo("\"attr1\" && \"attr2\""));
        assertThat(createZone.dataNodesAutoAdjustScaleUp(), equalTo(100));
        assertThat(createZone.dataNodesAutoAdjustScaleDown(), equalTo(200));
        assertThat(createZone.dataNodesAutoAdjust(), equalTo(300));
        assertThat(createZone.storageProfiles(), equalTo("lru_rocks, segmented_aipersist"));

        // Check option validation.
        node = parse("CREATE ZONE test with partitions=-1");
        expectOptionValidationError((SqlDdl) node, "PARTITION");

        node = parse("CREATE ZONE test with replicas=-1");
        expectOptionValidationError((SqlDdl) node, "REPLICAS");

        node = parse("CREATE ZONE test with storage_profiles='' ");
        expectOptionValidationError((SqlDdl) node, "STORAGE_PROFILES");
    }

    @Test
    public void testCreateZoneWithoutStorageProfileOptionShouldThrowError() throws SqlParseException {
        SqlNode node =  parse("CREATE ZONE test");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString(STORAGE_PROFILES + " option cannot be null"));

        SqlNode newNode =  parse("CREATE ZONE test with replicas=1");

        assertThat(newNode, instanceOf(SqlDdl.class));

        ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) newNode, createContext())
        );

        assertThat(ex.getMessage(), containsString(STORAGE_PROFILES + " option cannot be null"));
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

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneRenameCommand.class));

        AlterZoneRenameCommand zoneCmd = (AlterZoneRenameCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
        assertThat(zoneCmd.newZoneName(), equalTo("TEST2"));
        assertThat(zoneCmd.ifExists(), is(false));
    }

    @Test
    public void testRenameZoneIfExistCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE IF EXISTS test RENAME TO test2");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneRenameCommand.class));

        AlterZoneRenameCommand zoneCmd = (AlterZoneRenameCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
        assertThat(zoneCmd.newZoneName(), equalTo("TEST2"));
        assertThat(zoneCmd.ifExists(), is(true));
    }

    @Test
    public void testAlterZoneCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET replicas=3");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneSetCommand.class));

        AlterZoneSetCommand zoneCmd = (AlterZoneSetCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
        assertThat(zoneCmd.ifExists(), is(false));
    }

    @Test
    public void testAlterZoneIfExistsCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE IF EXISTS test SET replicas=3");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneSetCommand.class));

        AlterZoneSetCommand zoneCmd = (AlterZoneSetCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
        assertThat(zoneCmd.ifExists(), is(true));
    }

    @Test
    public void testAlterZoneSetCommand() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET "
                + "replicas=3, "
                + "partitions=8, "
                + "data_nodes_filter='\"attr1\" && \"attr2\"', "
                + "data_nodes_auto_adjust_scale_up=100, "
                + "data_nodes_auto_adjust_scale_down=200, "
                + "data_nodes_auto_adjust=300");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneSetCommand.class));

        AlterZoneSetCommand zoneCmd = (AlterZoneSetCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));

        assertThat(zoneCmd.replicas(), equalTo(3));
        assertThat(zoneCmd.partitions(), equalTo(8));
        assertThat(zoneCmd.nodeFilter(), equalTo("\"attr1\" && \"attr2\""));
        assertThat(zoneCmd.dataNodesAutoAdjustScaleUp(), equalTo(100));
        assertThat(zoneCmd.dataNodesAutoAdjustScaleDown(), equalTo(200));
        assertThat(zoneCmd.dataNodesAutoAdjust(), equalTo(300));
    }

    @Test
    public void testAlterZoneSetDefault() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET DEFAULT");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneSetDefaultCommand.class));

        AlterZoneSetDefaultCommand zoneCmd = (AlterZoneSetDefaultCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
        assertThat(zoneCmd.ifExists(), is(false));
    }

    @Test
    public void testAlterZoneSetDefaultIfExists() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE IF EXISTS test SET DEFAULT");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneSetDefaultCommand.class));

        AlterZoneSetDefaultCommand zoneCmd = (AlterZoneSetDefaultCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
        assertThat(zoneCmd.ifExists(), is(true));
    }

    @Test
    public void testAlterZoneCommandWithInvalidOptions() throws SqlParseException {
        SqlNode node = parse("ALTER ZONE test SET replicas=2, data_nodes_auto_adjust=-100");

        assertThat(node, instanceOf(SqlDdl.class));

        expectOptionValidationError((SqlDdl) node, "DATA_NODES_AUTO_ADJUST");
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

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(DropZoneCommand.class));

        DropZoneCommand zoneCmd = (DropZoneCommand) cmd;

        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
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

    private void expectOptionValidationError(SqlDdl node, String invalidOption) {
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert(node, createContext())
        );

        assertThat(ex.code(), equalTo(Sql.STMT_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("Zone option validation failed [option=" + invalidOption));
    }

    private void expectInvalidOptionType(SqlDdl node, String invalidOption) {
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert(node, createContext())
        );

        assertThat(ex.code(), equalTo(Sql.STMT_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("Invalid zone option type [option=" + invalidOption));
    }

    private void expectUnexpectedOption(SqlDdl node, String invalidOption) {
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert(node, createContext())
        );

        assertThat(ex.code(), equalTo(Sql.STMT_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("Unexpected zone option [option=" + invalidOption));
    }

    private void expectDuplicateOptionError(SqlDdl node, String option) {
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert(node, createContext())
        );

        assertThat(ex.code(), equalTo(Sql.STMT_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("Duplicate zone option has been specified [option=" + option));
    }
}
