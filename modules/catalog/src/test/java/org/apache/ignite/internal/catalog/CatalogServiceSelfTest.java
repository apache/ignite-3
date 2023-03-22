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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Catalog service self test.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = CatalogService.IGNITE_USE_CATALOG_PROPERTY, value = "true")
public class CatalogServiceSelfTest {
    private static final String TABLE_NAME = "myTable";
    private MetaStorageManager metaStorageManager;
    private CatalogServiceImpl catalogService;
    private VaultManager vaultManager;

    @BeforeEach
    public void initCatalogService() {
        vaultManager = new VaultManager(new InMemoryVaultService());
        metaStorageManager = StandaloneMetaStorageManager.create(vaultManager);
        catalogService = new CatalogServiceImpl(metaStorageManager);

        vaultManager.start();
        metaStorageManager.start();
        catalogService.start();

        try {
            metaStorageManager.deployWatches();
        } catch (NodeStoppingException e) {
            fail(e);
        }
    }

    @AfterEach
    public void cleanupResources() throws Exception {
        catalogService.stop();
        metaStorageManager.stop();
        vaultManager.stop();
    }

    @Test
    public void testEmptyCatalog() {
        assertNotNull(catalogService.activeSchema(System.currentTimeMillis()));
        assertNotNull(catalogService.schema(0));

        assertNull(catalogService.schema(1));
        assertThrows(IllegalStateException.class, () -> catalogService.activeSchema(-1L));

        assertNull(catalogService.table(0, System.currentTimeMillis()));
        assertNull(catalogService.index(0, System.currentTimeMillis()));

        SchemaDescriptor schema = catalogService.schema(0);
        assertEquals(CatalogUtils.DEFAULT_SCHEMA, schema.name());

        assertEquals(0, schema.version());
        assertEquals(0, schema.tables().length);
        assertEquals(0, schema.indexes().length);
    }

    @Test
    public void testCreateTable() {
        CreateTableParams params = CreateTableParams.builder()
                .schemaName("PUBLIC")
                .tableName(TABLE_NAME)
                .ifTableExists(true)
                .partitions(100)
                .replicas(10)
                .zone("ZONE")
                .columns(List.of(
                        new ColumnParams("key1", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("key2", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), true)
                ))
                .primaryKeyColumns(List.of("key1", "key2"))
                .colocationColumns(List.of("key2"))
                .dataStorage("STORAGE")
                .dataStorageOptions(Map.of("optKey", "optVal"))
                .build();

        CompletableFuture<?> fut = catalogService.createTable(params);

        assertThat(fut, CompletableFutureMatcher.willBe(true));

        // Validate catalog version from the past.
        SchemaDescriptor schema = catalogService.schema(0);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogUtils.DEFAULT_SCHEMA, schema.name());
        assertSame(schema, catalogService.activeSchema(0L));
        assertSame(schema, catalogService.activeSchema(123L));

        assertNull(schema.table(TABLE_NAME));
        assertNull(catalogService.table(TABLE_NAME, 123L));
        assertNull(catalogService.table(1, 123L));

        // Validate actual catalog
        schema = catalogService.schema(1);

        assertNotNull(schema);
        assertEquals(0, schema.id());
        assertEquals(CatalogUtils.DEFAULT_SCHEMA, schema.name());
        assertSame(schema, catalogService.activeSchema(System.currentTimeMillis()));

        assertSame(schema.table(TABLE_NAME), catalogService.table(TABLE_NAME, System.currentTimeMillis()));
        assertSame(schema.table(TABLE_NAME), catalogService.table(1, System.currentTimeMillis()));

        // Validate newly created table
        TableDescriptor table = schema.table(TABLE_NAME);

        assertEquals(1L, table.id());
        assertEquals(TABLE_NAME, table.name());
        assertEquals(0L, table.engineId());
        assertEquals(0L, table.zoneId());
    }

    @Test
    public void testCreateTableIfExistsFlag() {
        CreateTableParams params = CreateTableParams.builder()
                .tableName("table1")
                .columns(List.of(
                        new ColumnParams("key", ColumnType.INT32, DefaultValue.constant(null), false),
                        new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), false)
                ))
                .primaryKeyColumns(List.of("key"))
                .ifTableExists(true)
                .build();

        assertThat(catalogService.createTable(params), CompletableFutureMatcher.willBe(true));
        assertThat(catalogService.createTable(params), CompletableFutureMatcher.willBe(false));

        CompletableFuture<?> fut = catalogService.createTable(
                CreateTableParams.builder()
                        .tableName("table1")
                        .columns(List.of(
                                new ColumnParams("key", ColumnType.INT32, DefaultValue.constant(null), false),
                                new ColumnParams("val", ColumnType.INT32, DefaultValue.constant(null), false)
                        ))
                        .primaryKeyColumns(List.of("key"))
                        .ifTableExists(false)
                        .build());

        assertThat(fut, CompletableFutureExceptionMatcher.willThrowFast(TableAlreadyExistsException.class));
    }
}
