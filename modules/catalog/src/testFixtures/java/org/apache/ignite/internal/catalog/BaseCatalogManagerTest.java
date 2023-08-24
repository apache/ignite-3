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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.spy;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams.Builder;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for testing the {@link CatalogManager}.
 */
public abstract class BaseCatalogManagerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test";

    protected static final String TABLE_NAME = "test_table";

    protected static final String INDEX_NAME = "myIndex";

    final HybridClock clock = new HybridClockImpl();

    private VaultManager vault;

    private MetaStorageManager metastore;

    UpdateLog updateLog;

    ClockWaiter clockWaiter;

    protected CatalogManagerImpl manager;

    @BeforeEach
    void setUp() {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(NODE_NAME));

        updateLog = spy(new UpdateLogImpl(metastore));
        clockWaiter = spy(new ClockWaiter(NODE_NAME, clock));

        manager = new CatalogManagerImpl(updateLog, clockWaiter);

        vault.start();
        metastore.start();
        clockWaiter.start();
        manager.start();

        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(Stream.of(manager, clockWaiter, metastore, vault)
                .filter(Objects::nonNull)
                .map(component -> component::stop)
        );
    }

    protected static CreateHashIndexParams createHashIndexParams(
            String indexName,
            boolean uniq,
            @Nullable List<String> indexColumns
    ) {
        CreateHashIndexParams.Builder builder = CreateHashIndexParams.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .indexName(indexName);

        builder.unique(uniq);

        if (indexColumns != null) {
            builder.columns(indexColumns);
        }

        return builder.build();
    }

    protected static CreateHashIndexParams createHashIndexParams(
            String indexName,
            @Nullable List<String> indexColumns
    ) {
        return createHashIndexParams(indexName, false, indexColumns);
    }

    protected static CreateSortedIndexParams createSortedIndexParams(
            String indexName,
            boolean uniq,
            @Nullable List<String> indexColumns,
            @Nullable List<CatalogColumnCollation> columnsCollations
    ) {
        CreateSortedIndexParams.Builder builder = CreateSortedIndexParams.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .indexName(indexName);

        builder.unique(uniq);

        if (indexColumns != null) {
            builder.columns(indexColumns);
        }

        if (columnsCollations != null) {
            builder.collations(columnsCollations);
        }

        return builder.build();
    }

    protected static CreateSortedIndexParams createSortedIndexParams(
            String indexName,
            @Nullable List<String> indexColumns,
            @Nullable List<CatalogColumnCollation> columnsCollations
    ) {
        return createSortedIndexParams(indexName, false, indexColumns, columnsCollations);
    }

    protected static CreateTableParams createTableParams(
            String tableName,
            @Nullable List<ColumnParams> columns,
            @Nullable List<String> primaryKeys,
            @Nullable List<String> colocationColumns
    ) {
        Builder builder = CreateTableParams.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .zone(DEFAULT_ZONE_NAME)
                .tableName(tableName);

        if (columns != null) {
            builder.columns(columns);
        }

        if (primaryKeys != null) {
            builder.primaryKeyColumns(primaryKeys);
        }

        if (colocationColumns != null) {
            builder.colocationColumns(colocationColumns);
        }

        return builder.build();
    }

    protected static ColumnParams columnParams(String name, ColumnType type) {
        return columnParams(name, type, false);
    }

    protected static ColumnParams columnParams(String name, ColumnType type, boolean nullable) {
        return columnParamsBuilder(name, type, nullable).build();
    }

    protected static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type) {
        return columnParamsBuilder(name, type, false);
    }

    protected static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type);
    }

    protected static DropTableParams dropTableParams(String tableName) {
        return DropTableParams.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(tableName).build();
    }

    protected static AlterTableDropColumnParams dropColumnParams(String... columns) {
        return AlterTableDropColumnParams.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(TABLE_NAME).columns(Set.of(columns)).build();
    }

    protected static AlterTableAddColumnParams addColumnParams(ColumnParams... columns) {
        return AlterTableAddColumnParams.builder().schemaName(DEFAULT_SCHEMA_NAME).tableName(TABLE_NAME).columns(List.of(columns)).build();
    }
}
