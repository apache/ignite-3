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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.junit.jupiter.api.Test;

/**
 * For {@link DdlToCatalogCommandConverter} testing.
 */
public class DdlToCatalogCommandConverterTest {
    private static final String ZONE_NAME = "zone_test";

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
}
