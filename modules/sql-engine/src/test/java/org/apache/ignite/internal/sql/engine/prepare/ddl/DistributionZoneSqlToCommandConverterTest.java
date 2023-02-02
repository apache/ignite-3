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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Tests the conversion of a sql zone definition to a command.
 */
public class DistributionZoneSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {
    @Test
    public void testCreateZone() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test");

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
                + "data_nodes_auto_adjust=300");

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

        // Check option validation.
        node = parse("CREATE ZONE test with partitions=-1");
        expectOptionValidationError((SqlDdl) node, "PARTITION");

        node = parse("CREATE ZONE test with replicas=-1");
        expectOptionValidationError((SqlDdl) node, "REPLICAS");
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
                + "data_nodes_filter='\"attr1\" && \"attr2\"', "
                + "data_nodes_auto_adjust_scale_up=100, "
                + "data_nodes_auto_adjust_scale_down=200, "
                + "data_nodes_auto_adjust=300");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, Matchers.instanceOf(AlterZoneSetCommand.class));

        AlterZoneSetCommand zoneCmd = (AlterZoneSetCommand) cmd;
        assertThat(zoneCmd.zoneName(), equalTo("TEST"));

        assertThat(zoneCmd.replicas(), equalTo(3));
        assertThat(zoneCmd.nodeFilter(), equalTo("\"attr1\" && \"attr2\""));
        assertThat(zoneCmd.dataNodesAutoAdjustScaleUp(), equalTo(100));
        assertThat(zoneCmd.dataNodesAutoAdjustScaleDown(), equalTo(200));
        assertThat(zoneCmd.dataNodesAutoAdjust(), equalTo(300));
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

    private void expectOptionValidationError(SqlDdl node, String invalidOption) {
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert(node, createContext())
        );

        assertThat(ex.code(), equalTo(Sql.QUERY_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("Zone option validation failed [option=" + invalidOption));
    }

    private void expectDuplicateOptionError(SqlDdl node, String option) {
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert(node, createContext())
        );

        assertThat(ex.code(), equalTo(Sql.QUERY_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("Duplicate DDL command option has been specified [option=" + option));
    }
}
