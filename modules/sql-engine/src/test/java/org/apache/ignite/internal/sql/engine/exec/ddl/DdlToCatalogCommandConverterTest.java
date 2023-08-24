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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.constant;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.junit.jupiter.api.Test;

/**
 * For {@link DdlToCatalogCommandConverter} testing.
 */
public class DdlToCatalogCommandConverterTest {
    private static final String ZONE_NAME = "zone_test";

    private static final String TABLE_NAME = "table_test";

    @Test
    void testConvertCreateZoneCommand() {
        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.partitions(1);
        cmd.replicas(2);
        cmd.dataNodesAutoAdjust(3);
        cmd.dataNodesAutoAdjustScaleUp(4);
        cmd.dataNodesAutoAdjustScaleDown(5);
        cmd.nodeFilter("filter");
        cmd.dataStorage("test_engine");
        cmd.addDataStorageOption("dataRegion", "test_region");

        CreateZoneParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(ZONE_NAME, params.zoneName());
        assertEquals(1, params.partitions());
        assertEquals(2, params.replicas());
        assertEquals(3, params.dataNodesAutoAdjust());
        assertEquals(4, params.dataNodesAutoAdjustScaleUp());
        assertEquals(5, params.dataNodesAutoAdjustScaleDown());
        assertEquals("filter", params.filter());
        assertEquals("test_engine", params.dataStorage().engine());
        assertEquals("test_region", params.dataStorage().dataRegion());
    }

    @Test
    void testConvertCreateZoneCommandWithDefaults() {
        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);

        CreateZoneParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(ZONE_NAME, params.zoneName());
        assertNull(params.partitions());
        assertNull(params.replicas());
        assertNull(params.dataNodesAutoAdjust());
        assertNull(params.dataNodesAutoAdjustScaleUp());
        assertNull(params.dataNodesAutoAdjustScaleDown());
        assertNull(params.filter());
        assertEquals(DEFAULT_STORAGE_ENGINE, params.dataStorage().engine());
        assertEquals(DEFAULT_DATA_REGION, params.dataStorage().dataRegion());
    }

    @Test
    void testConvertCreateZoneCommandWithMissingDataRegion() {
        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.dataStorage("test_storage");

        CreateZoneParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(DEFAULT_DATA_REGION, params.dataStorage().dataRegion());
    }

    @Test
    void testConvertCreateZoneCommandWithWrongDataRegionParam() {
        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.addDataStorageOption("wrongDataRegionParam", "test_region");

        CreateZoneParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(DEFAULT_DATA_REGION, params.dataStorage().dataRegion());
    }

    @Test
    void testConvertAlterZoneCommand() {
        AlterZoneSetCommand cmd = new AlterZoneSetCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.partitions(1);
        cmd.replicas(2);
        cmd.dataNodesAutoAdjust(3);
        cmd.dataNodesAutoAdjustScaleUp(4);
        cmd.dataNodesAutoAdjustScaleDown(5);
        cmd.nodeFilter("filter");

        AlterZoneParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(ZONE_NAME, params.zoneName());
        assertEquals(1, params.partitions());
        assertEquals(2, params.replicas());
        assertEquals(3, params.dataNodesAutoAdjust());
        assertEquals(4, params.dataNodesAutoAdjustScaleUp());
        assertEquals(5, params.dataNodesAutoAdjustScaleDown());
        assertEquals("filter", params.filter());
    }

    @Test
    void testConvertDropZoneCommand() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName(ZONE_NAME);

        DropZoneParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(ZONE_NAME, params.zoneName());
    }

    @Test
    void testConvertRenameZoneCommand() {
        AlterZoneRenameCommand cmd = new AlterZoneRenameCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.newZoneName(ZONE_NAME + "_new");

        RenameZoneParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(ZONE_NAME, params.zoneName());
        assertEquals(ZONE_NAME + "_new", params.newZoneName());
    }

    @Test
    void testConvertCreateTableCommand() {
        CreateTableCommand cmd = new CreateTableCommand();
        cmd.schemaName(DEFAULT_SCHEMA_NAME);
        cmd.zone(ZONE_NAME);
        cmd.tableName(TABLE_NAME);
        cmd.columns(List.of(new ColumnDefinition("key", createRelDataType(INTEGER), constant(0))));
        cmd.primaryKeyColumns(List.of("key"));
        cmd.colocationColumns(List.of("key"));

        CreateTableParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(DEFAULT_SCHEMA_NAME, params.schemaName());
        assertEquals(ZONE_NAME, params.zone());
        assertEquals(TABLE_NAME, params.tableName());
        assertEquals(List.of("key"), params.primaryKeyColumns());
        assertEquals(List.of("key"), params.colocationColumns());

        // Check columns.
        assertThat(params.columns(), hasSize(1));

        ColumnParams column = params.columns().get(0);
        assertEquals("key", column.name());
        assertEquals(INT32, column.type());
    }

    @Test
    void testConvertCreateTableCommandWithoutColocationColumns() {
        CreateTableCommand cmd = new CreateTableCommand();
        cmd.schemaName(DEFAULT_SCHEMA_NAME);
        cmd.zone(ZONE_NAME);
        cmd.tableName(TABLE_NAME);
        cmd.columns(List.of(new ColumnDefinition("key", createRelDataType(INTEGER), constant(0))));
        cmd.primaryKeyColumns(List.of("key"));

        CreateTableParams params = DdlToCatalogCommandConverter.convert(cmd);

        assertEquals(List.of("key"), params.colocationColumns());
    }

    private static RelDataType createRelDataType(SqlTypeName type) {
        RelDataType mock = mock(RelDataType.class);

        when(mock.getSqlTypeName()).thenReturn(type);

        return mock;
    }
}
