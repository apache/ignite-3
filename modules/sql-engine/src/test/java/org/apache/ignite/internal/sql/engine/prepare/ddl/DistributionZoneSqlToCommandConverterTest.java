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
    public void create() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test");

        assertThat(node, instanceOf(SqlDdl.class));

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateZoneCommand.class));

        CreateZoneCommand zoneCmd = (CreateZoneCommand) cmd;

        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
    }

    @Test
    public void createWithOptions() throws SqlParseException {
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
        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) parse("CREATE ZONE test with partitions=-1"), createContext())
        );

        assertThat(ex.code(), equalTo(Sql.QUERY_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("DDL option validation failed [option=PARTITIONS"));

        ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) parse("CREATE ZONE test with replicas=-1"), createContext())
        );

        assertThat(ex.code(), equalTo(Sql.QUERY_VALIDATION_ERR));
        assertThat(ex.getMessage(), containsString("DDL option validation failed [option=REPLICAS"));
    }

    @Test
    public void createWithDuplicateOptions() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test with partitions=2, replicas=0, PARTITIONS=1");

        assertThat(node, instanceOf(SqlDdl.class));

        IgniteException ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.code(), equalTo(Sql.QUERY_VALIDATION_ERR));
    }

    @Test
    public void drop() throws SqlParseException {
        SqlNode node = parse("DROP ZONE test");

        assertThat(node, instanceOf(SqlDdl.class));

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(DropZoneCommand.class));

        DropZoneCommand zoneCmd = (DropZoneCommand) cmd;

        assertThat(zoneCmd.zoneName(), equalTo("TEST"));
    }
}
