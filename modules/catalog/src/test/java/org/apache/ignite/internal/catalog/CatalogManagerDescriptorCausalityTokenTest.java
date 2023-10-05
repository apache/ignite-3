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

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.addColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.dropColumnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.dropTableCommand;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.junit.jupiter.api.Test;

/**
 * Test for checking that catalog descriptors entities' "update token" are updated after general catalog operations.
 */
public class CatalogManagerDescriptorCausalityTokenTest extends BaseCatalogManagerTest {
    private static final String SCHEMA_NAME = DEFAULT_SCHEMA_NAME;
    private static final String ZONE_NAME = DEFAULT_ZONE_NAME;
    private static final String TABLE_NAME_2 = "myTable2";
    private static final String NEW_COLUMN_NAME = "NEWCOL";

    @Test
    public void testEmptyCatalog() {
        CatalogSchemaDescriptor defaultSchema = manager.schema(DEFAULT_SCHEMA_NAME, 0);

        assertNotNull(defaultSchema);
        assertSame(defaultSchema, manager.activeSchema(DEFAULT_SCHEMA_NAME, clock.nowLong()));
        assertSame(defaultSchema, manager.schema(0));
        assertSame(defaultSchema, manager.activeSchema(clock.nowLong()));

        assertNull(manager.schema(1));
        assertThrows(IllegalStateException.class, () -> manager.activeSchema(-1L));

        // Validate default schema.
        assertEquals(INITIAL_CAUSALITY_TOKEN, defaultSchema.updateToken());

        // Default distribution zone must exists.
        CatalogZoneDescriptor zone = manager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        assertNotNull(zone);

        assertEquals(INITIAL_CAUSALITY_TOKEN, zone.updateToken());
    }

    @Test
    public void testCreateTable() {
        assertThat(
                manager.execute(createTableCommand(
                        TABLE_NAME,
                        List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)),
                        List.of("key1", "key2"),
                        List.of("key2")
                )),
                willBe(nullValue())
        );

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(0);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(123L));
        assertEquals(INITIAL_CAUSALITY_TOKEN, schema.updateToken());

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, 123L));

        // Validate actual catalog.
        schema = manager.schema(SCHEMA_NAME, 1);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        // Validate newly created table.
        assertEquals(TABLE_NAME, table.name());
        assertTrue(table.updateToken() > INITIAL_CAUSALITY_TOKEN);
        assertEquals(table.updateToken(), schema.updateToken());

        // Validate another table creation.
        assertThat(manager.execute(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        // Validate actual catalog. has both tables.
        schema = manager.schema(2);
        table = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertSame(table, manager.table(TABLE_NAME, clock.nowLong()));
        assertSame(table, manager.table(table.id(), clock.nowLong()));

        assertSame(table2, manager.table(TABLE_NAME_2, clock.nowLong()));
        assertSame(table2, manager.table(table2.id(), clock.nowLong()));

        assertNotSame(table, table2);

        // Assert that causality token of the last update of table2 is greater than for earlier created table.
        assertTrue(table2.updateToken() > table.updateToken());

        assertEquals(table2.updateToken(), schema.updateToken());
    }

    @Test
    public void testDropTable() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));
        assertThat(manager.execute(simpleTable(TABLE_NAME_2)), willBe(nullValue()));

        long beforeDropTimestamp = clock.nowLong();

        assertThat(manager.execute(dropTableCommand(TABLE_NAME)), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(2);
        CatalogTableDescriptor table1 = schema.table(TABLE_NAME);
        CatalogTableDescriptor table2 = schema.table(TABLE_NAME_2);

        assertNotEquals(table1.id(), table2.id());

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(beforeDropTimestamp));

        long causalityToken = schema.updateToken();
        assertTrue(causalityToken > INITIAL_CAUSALITY_TOKEN);

        assertSame(table1, manager.table(TABLE_NAME, beforeDropTimestamp));
        assertSame(table1, manager.table(table1.id(), beforeDropTimestamp));

        assertSame(table2, manager.table(TABLE_NAME_2, beforeDropTimestamp));
        assertSame(table2, manager.table(table2.id(), beforeDropTimestamp));

        // Validate actual catalog.
        schema = manager.schema(3);

        assertNotNull(schema);
        assertEquals(SCHEMA_NAME, schema.name());
        assertSame(schema, manager.activeSchema(clock.nowLong()));

        assertNull(schema.table(TABLE_NAME));
        assertNull(manager.table(TABLE_NAME, clock.nowLong()));
        assertNull(manager.table(table1.id(), clock.nowLong()));

        // Assert that drop table changes schema's last update token.
        assertTrue(schema.updateToken() > causalityToken);
    }

    @Test
    public void testAddColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(
                manager.execute(addColumnParams(TABLE_NAME,
                        columnParamsBuilder(NEW_COLUMN_NAME, STRING, 11, true).defaultValue(constant("Ignite!")).build()
                )),
                willBe(nullValue())
        );

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        assertNotNull(table);

        long schemaCausalityToken = schema.updateToken();
        assertTrue(schemaCausalityToken > INITIAL_CAUSALITY_TOKEN);
        assertEquals(schemaCausalityToken, table.updateToken());

        assertNull(schema.table(TABLE_NAME).column(NEW_COLUMN_NAME));

        // Validate actual catalog.
        schema = manager.activeSchema(clock.nowLong());
        assertNotNull(schema);
        table = schema.table(TABLE_NAME);
        assertNotNull(table);

        // Validate column descriptor.
        CatalogTableColumnDescriptor column = schema.table(TABLE_NAME).column(NEW_COLUMN_NAME);
        assertEquals(NEW_COLUMN_NAME, column.name());

        // Assert that schema's and table's update token was updated after adding a column.
        assertTrue(schema.updateToken() > schemaCausalityToken);
        assertEquals(schema.updateToken(), table.updateToken());
    }

    @Test
    public void testDropColumn() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        long beforeAddedTimestamp = clock.nowLong();

        assertThat(manager.execute(dropColumnParams(TABLE_NAME, "VAL")), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.activeSchema(beforeAddedTimestamp);
        assertNotNull(schema);
        CatalogTableDescriptor table = schema.table(TABLE_NAME);
        assertNotNull(table);

        long schemaCausalityToken = schema.updateToken();
        assertTrue(schemaCausalityToken > INITIAL_CAUSALITY_TOKEN);
        assertEquals(schemaCausalityToken, table.updateToken());

        assertNotNull(schema.table(TABLE_NAME).column("VAL"));

        // Validate actual catalog.
        schema = manager.activeSchema(clock.nowLong());
        assertNotNull(schema);
        table = schema.table(TABLE_NAME);
        assertNotNull(table);

        assertNull(schema.table(TABLE_NAME).column("VAL"));

        // Assert that schema's and table's update token was updated after dropping a column.
        assertTrue(schema.updateToken() > schemaCausalityToken);
        assertEquals(schema.updateToken(), table.updateToken());
    }

    @Test
    public void testCreateHashIndex() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        assertThat(manager.execute(createHashIndexCommand(INDEX_NAME, List.of("VAL", "ID"))), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, 123L));

        long schemaCausalityToken = schema.updateToken();

        assertTrue(schemaCausalityToken > INITIAL_CAUSALITY_TOKEN);

        // Validate actual catalog.
        schema = manager.schema(2);

        CatalogHashIndexDescriptor index = (CatalogHashIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));
        assertTrue(schema.updateToken() > schemaCausalityToken);

        // Validate newly created hash index.
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(schema.updateToken(), index.updateToken());
    }

    @Test
    public void testCreateSortedIndex() {
        assertThat(manager.execute(simpleTable(TABLE_NAME)), willBe(nullValue()));

        CatalogCommand command = createSortedIndexCommand(
                INDEX_NAME,
                true,
                List.of("VAL", "ID"),
                List.of(DESC_NULLS_FIRST, ASC_NULLS_LAST)
        );

        assertThat(manager.execute(command), willBe(nullValue()));

        // Validate catalog version from the past.
        CatalogSchemaDescriptor schema = manager.schema(1);

        assertNotNull(schema);
        assertNull(schema.index(INDEX_NAME));
        assertNull(manager.index(INDEX_NAME, 123L));

        long schemaCausalityToken = schema.updateToken();
        assertTrue(schemaCausalityToken > INITIAL_CAUSALITY_TOKEN);

        // Validate actual catalog.
        schema = manager.schema(2);

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) schema.index(INDEX_NAME);

        assertNotNull(schema);
        assertSame(index, manager.index(INDEX_NAME, clock.nowLong()));
        assertSame(index, manager.index(index.id(), clock.nowLong()));
        assertTrue(schema.updateToken() > schemaCausalityToken);

        // Validate newly created sorted index.
        assertEquals(INDEX_NAME, index.name());
        assertEquals(schema.table(TABLE_NAME).id(), index.tableId());
        assertEquals(schema.updateToken(), index.updateToken());
    }

    @Test
    public void testCreateZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams params = CreateZoneParams.builder().zoneName(zoneName).build();

        assertThat(manager.createZone(params), willCompleteSuccessfully());

        // Validate catalog version from the past.
        assertNull(manager.zone(zoneName, 0));
        assertNull(manager.zone(zoneName, 123L));

        // Validate actual catalog.
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());

        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        // Validate newly created zone.
        assertEquals(zoneName, zone.name());
        assertTrue(zone.updateToken() > INITIAL_CAUSALITY_TOKEN);
    }

    @Test
    public void testRenameZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createParams = CreateZoneParams.builder().zoneName(zoneName).build();

        assertThat(manager.createZone(createParams), willCompleteSuccessfully());

        long beforeDropTimestamp = clock.nowLong();

        String newZoneName = "RenamedZone";

        RenameZoneParams renameZoneParams = RenameZoneParams.builder()
                .zoneName(zoneName)
                .newZoneName(newZoneName)
                .build();

        assertThat(manager.renameZone(renameZoneParams), willCompleteSuccessfully());

        // Validate catalog version from the past.
        CatalogZoneDescriptor zone = manager.zone(zoneName, beforeDropTimestamp);

        assertNotNull(zone);
        assertEquals(zoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), beforeDropTimestamp));
        long causalityToken = zone.updateToken();
        assertTrue(causalityToken > INITIAL_CAUSALITY_TOKEN);

        // Validate actual catalog.
        zone = manager.zone(newZoneName, clock.nowLong());

        assertNotNull(zone);
        assertNull(manager.zone(zoneName, clock.nowLong()));
        assertEquals(newZoneName, zone.name());

        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));
        // Assert that renaming of a zone updates token.
        assertTrue(zone.updateToken() > causalityToken);
    }

    @Test
    public void testAlterZone() {
        String zoneName = ZONE_NAME + 1;

        CreateZoneParams createParams = CreateZoneParams.builder()
                .zoneName(zoneName)
                .filter("expression")
                .build();

        AlterZoneParams alterZoneParams = AlterZoneParams.builder()
                .zoneName(zoneName)
                .dataNodesAutoAdjustScaleUp(3)
                .dataNodesAutoAdjustScaleDown(4)
                .filter("newExpression")
                .build();

        assertThat(manager.createZone(createParams), willCompleteSuccessfully());
        CatalogZoneDescriptor zone = manager.zone(zoneName, clock.nowLong());
        assertNotNull(zone);
        long causalityToken = zone.updateToken();
        assertTrue(causalityToken > INITIAL_CAUSALITY_TOKEN);

        assertThat(manager.alterZone(alterZoneParams), willCompleteSuccessfully());

        // Validate actual catalog.
        zone = manager.zone(zoneName, clock.nowLong());
        assertNotNull(zone);
        assertSame(zone, manager.zone(zone.id(), clock.nowLong()));

        assertEquals(zoneName, zone.name());
        assertEquals(3, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(4, zone.dataNodesAutoAdjustScaleDown());
        assertEquals("newExpression", zone.filter());
        // Assert that altering of a zone updates token.
        assertTrue(zone.updateToken() > causalityToken);
    }
}
