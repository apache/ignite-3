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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.addColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.dropColumnParams;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Test for checking that catalog descriptors entities' "update token" are updated after general catalog operations.
 */
public class CatalogManagerDescriptorCausalityTokenTest extends BaseCatalogManagerTest {
    private static final String ZONE_NAME = "TEST_ZONE_NAME";
    private static final String TABLE_NAME_2 = "myTable2";
    private static final String NEW_COLUMN_NAME = "NEWCOL";

    @Test
    public void testEmptyCatalog() {
        Catalog catalog = manager.activeCatalog(clock.nowLong());

        CatalogSchemaDescriptor defaultSchema = catalog.schema(SCHEMA_NAME);

        assertNotNull(defaultSchema);

        assertTrue(
                defaultSchema.updateTimestamp().longValue() > INITIAL_TIMESTAMP.longValue(),
                "Non default timestamp was expected"
        );
    }

    @Test
    public void testCreateTable() {
        long beforeTableCreated = clock.nowLong();

        tryApplyAndExpectApplied(createTableCommand(
                TABLE_NAME,
                List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)),
                List.of("key1", "key2"),
                List.of("key2")));

        // Validate catalog version from the past.
        Catalog catalog = manager.activeCatalog(beforeTableCreated);
        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        HybridTimestamp timestamp = metastore.timestampByRevisionLocally(2);
        assertEquals(timestamp, schema.updateTimestamp());

        assertNull(schema.table(TABLE_NAME));

        // Validate actual catalog.
        schema = manager.activeCatalog(clock.nowLong()).schema(SCHEMA_NAME);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());

        // Validate newly created table.
        assertEquals(TABLE_NAME, table.name());
        assertTrue(table.updateTimestamp().longValue() > INITIAL_TIMESTAMP.longValue());
        assertEquals(table.updateTimestamp(), schema.updateTimestamp());

        // Validate another table creation.
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME_2));

        // Validate actual catalog. has both tables.
        catalog = manager.activeCatalog(clock.nowLong());
        schema = catalog.schema(SCHEMA_NAME);
        table = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());

        assertSame(table, catalog.table(table.id()));
        assertSame(table2, catalog.table(table2.id()));

        assertNotSame(table, table2);

        // Assert that causality token of the last update of table2 is greater than for earlier created table.
        assertTrue(table2.updateTimestamp().longValue() > table.updateTimestamp().longValue());

        assertEquals(table2.updateTimestamp(), schema.updateTimestamp());
    }

    @Test
    public void testDropTable() {
        tryApplyAndExpectApplied(simpleTable(TABLE_NAME));
        await(manager.execute(simpleTable(TABLE_NAME_2)));

        long beforeDropTimestamp = clock.nowLong();

        await(manager.execute(dropTableCommand(TABLE_NAME)));

        // Validate catalog version from the past.
        Catalog catalog = manager.activeCatalog(beforeDropTimestamp);
        CatalogSchemaDescriptor schema = catalog.schema(SCHEMA_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());

        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);

        assertNotEquals(table1.id(), table2.id());

        HybridTimestamp timestamp = schema.updateTimestamp();
        assertTrue(timestamp.longValue() > INITIAL_TIMESTAMP.longValue());

        // Validate actual catalog.
        schema = manager.activeCatalog(clock.nowLong()).schema(SCHEMA_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());

        assertNull(schema.table(TABLE_NAME));

        // Assert that drop table changes schema's last update token.
        assertTrue(schema.updateTimestamp().longValue() > timestamp.longValue());
    }

    @Test
    public void testAddColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(
                manager.execute(addColumnParams(TABLE_NAME,
                        columnParamsBuilder(NEW_COLUMN_NAME, STRING, 11, true).defaultValue(constant("Ignite!")).build()
                )),
                willCompleteSuccessfully()
        );

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeCatalog(beforeAddedTimestamp).schema(SCHEMA_NAME);
        assertNotNull(schema);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        assertNotNull(table);

        HybridTimestamp schemaTimestamp = schema.updateTimestamp();
        assertTrue(schemaTimestamp.longValue() > INITIAL_TIMESTAMP.longValue());
        assertEquals(schemaTimestamp, table.updateTimestamp());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Validate actual catalog.
        schema = manager.activeCatalog(clock.nowLong()).schema(SCHEMA_NAME);
        assertNotNull(schema);
        table = schema.table(TABLE_NAME);
        assertNotNull(table);

        // Validate column descriptor.
        CatalogTableColumnDescriptor column = schema.table(TABLE_NAME).column(NEW_COLUMN_NAME);
        assertEquals(NEW_COLUMN_NAME, column.name());

        // Assert that schema's and table's update token was updated after adding a column.
        assertTrue(schema.updateTimestamp().longValue() > schemaTimestamp.longValue());
        assertEquals(schema.updateTimestamp(), table.updateTimestamp());
    }

    @Test
    public void testDropColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willCompleteSuccessfully());

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "VAL")), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeCatalog(beforeAddedTimestamp).schema(SCHEMA_NAME);
        assertNotNull(schema);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        assertNotNull(table);

        HybridTimestamp schemaTimestamp = schema.updateTimestamp();
        assertTrue(schemaTimestamp.longValue() > INITIAL_TIMESTAMP.longValue());
        assertEquals(schemaTimestamp, table.updateTimestamp());

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));

        // Validate actual catalog.
        schema = manager.activeCatalog(clock.nowLong()).schema(SCHEMA_NAME);
        assertNotNull(schema);
        table = schema.table(TABLE_NAME);
        assertNotNull(table);

        assertNull(schema.table(TABLE_NAME).column("VAL"));

        // Assert that schema's and table's update token was updated after dropping a column.
        assertTrue(schema.updateTimestamp().longValue() > schemaTimestamp.longValue());
        assertEquals(schema.updateTimestamp(), table.updateTimestamp());
    }

    @Test
    public void testCreateHashIndex() {
        await(manager.execute(simpleTable(TABLE_NAME)));

        long beforeIndexCreated = clock.nowLong();

        await(manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL", "ID"))));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeCatalog(beforeIndexCreated).schema(SCHEMA_NAME);

        assertNotNull(schema);
        assertNull(schema.aliveIndex(INDEX_NAME));

        HybridTimestamp schemaTimestamp = schema.updateTimestamp();

        assertTrue(schemaTimestamp.longValue() > INITIAL_TIMESTAMP.longValue());

        // Validate actual catalog.
        schema = manager.activeCatalog(clock.nowLong()).schema(SCHEMA_NAME);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) schema.aliveIndex(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, schema.aliveIndex(INDEX_NAME));
        assertTrue(schema.updateTimestamp().longValue() > schemaTimestamp.longValue());

        // Validate newly created hash index.
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(schema.updateTimestamp(), index.updateTimestamp());
    }

    @Test
    public void testCreateSortedIndex() {
        await(manager.execute(simpleTable(TABLE_NAME)));

        long beforeIndexCreated = clock.nowLong();

        CatalogCommand command = createSortedIndexCommand(
                INDEX_NAME,
                true,
                List.of("VAL", "ID"),
                List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST)
        );

        await(manager.execute(command));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeCatalog(beforeIndexCreated).schema(SCHEMA_NAME);

        assertNotNull(schema);
        assertNull(schema.aliveIndex(INDEX_NAME));

        HybridTimestamp schemaTimestamp = schema.updateTimestamp();
        assertTrue(schemaTimestamp.longValue() > INITIAL_TIMESTAMP.longValue());

        // Validate actual catalog.
        schema = manager.activeCatalog(clock.nowLong()).schema(SCHEMA_NAME);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) schema.aliveIndex(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, schema.aliveIndex(INDEX_NAME));
        assertTrue(schema.updateTimestamp().longValue() > schemaTimestamp.longValue());

        // Validate newly created sorted index.
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(schema.updateTimestamp(), index.updateTimestamp());
    }

    @Test
    public void testCreateZone() {
        String zoneName = ZONE_NAME;

        long beforeZoneCreated = clock.nowLong();

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        // Validate catalog version from the past.
        assertNull(manager.activeCatalog(beforeZoneCreated).zone(zoneName));

        // Validate actual catalog.
        Catalog catalog = manager.activeCatalog(clock.nowLong());
        CatalogZoneDescriptor zone = catalog.zone(zoneName);

        assertNotNull(zone);
        assertSame(zone, catalog.zone(zone.id()));

        // Validate newly created zone.
        assertEquals(zoneName, zone.name());
        assertTrue(zone.updateTimestamp().longValue() > INITIAL_TIMESTAMP.longValue());
    }

    @Test
    public void testRenameZone() {
        String zoneName = ZONE_NAME;

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        String newZoneName = "RenamedZone";

        CatalogCommand renameZoneCmd = RenameZoneCommand.builder()
                .zoneName(zoneName)
                .newZoneName(newZoneName)
                .build();

        assertThat(manager.execute(renameZoneCmd), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.activeCatalog(beforeDropTimestamp).zone(zoneName);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.activeCatalog(beforeDropTimestamp).zone(zone.id()));
        HybridTimestamp updateTimestamp = zone.updateTimestamp();
        assertTrue(updateTimestamp.longValue() > INITIAL_TIMESTAMP.longValue());

        // Validate actual catalog.
        Catalog catalog = manager.activeCatalog(clock.nowLong());

        zone = catalog.zone(newZoneName);

        assertNotNull(zone);
        assertNull(catalog.zone(zoneName));
        assertEquals(newZoneName, zone.name());

        assertSame(zone, catalog.zone(zone.id()));
        // Assert that renaming of a zone updates token.
        assertTrue(zone.updateTimestamp().longValue() > updateTimestamp.longValue());
    }

    @Test
    public void testAlterZone() {
        String zoneName = ZONE_NAME;

        CatalogCommand alterCmd = AlterZoneCommand.builder()
                .zoneName(zoneName)
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .filter("expression")
                .build();

        assertThat(manager.execute(cmd), willCompleteSuccessfully());
        CatalogZoneDescriptor zone = manager.activeCatalog(clock.nowLong()).zone(zoneName);
        assertNotNull(zone);
        HybridTimestamp updateTimestamp = zone.updateTimestamp();
        assertTrue(updateTimestamp.longValue() > INITIAL_TIMESTAMP.longValue());

        assertThat(manager.execute(alterCmd), willCompleteSuccessfully());

        // Validate actual catalog.
        Catalog catalog = manager.activeCatalog(clock.nowLong());
        zone = catalog.zone(zoneName);
        assertNotNull(zone);
        assertSame(zone, catalog.zone(zone.id()));

        assertEquals(zoneName, zone.name());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
        // Assert that altering of a zone updates token.
        assertTrue(zone.updateTimestamp().longValue() > updateTimestamp.longValue());
    }
}
