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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.DESC_NULLS_FIRST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test class to verify {@link IndexManager}.
 */
@ExtendWith(ConfigurationExtension.class)
public class IndexManagerTest {
    private static final String TABLE_NAME = "tName";

    @InjectConfiguration(
            "mock.tables.tName {"
                    + "id: 1, "
                    + "columns.c1 {type.type: STRING}, "
                    + "columns.c2 {type.type: STRING}, "
                    + "primaryKey {columns: [c1], colocationColumns: [c1]}"
                    + "}"
    )
    private TablesConfiguration tablesConfig;

    private final HybridClock clock = new HybridClockImpl();

    private VaultManager vaultManager;

    private MetaStorageManager metaStorageManager;

    private ClockWaiter clockWaiter;

    private CatalogManager catalogManager;

    private IndexManager indexManager;

    @BeforeEach
    public void setUp() {
        TableManager tableManagerMock = mock(TableManager.class);

        when(tableManagerMock.tableAsync(anyLong(), anyString())).thenAnswer(inv -> {
            InternalTable tbl = mock(InternalTable.class);

            doReturn(catalogManager.table(inv.getArgument(1), clock.nowLong()).id()).when(tbl).tableId();

            return completedFuture(new TableImpl(tbl, new HeapLockManager()));
        });

        when(tableManagerMock.getTable(anyString())).thenAnswer(inv -> {
            InternalTable tbl = mock(InternalTable.class);

            doReturn(catalogManager.table(inv.getArgument(0), clock.nowLong()).id()).when(tbl).tableId();

            return new TableImpl(tbl, new HeapLockManager());
        });

        when(tableManagerMock.localPartitionSetAsync(anyLong(), anyInt())).thenReturn(completedFuture(PartitionSet.EMPTY_SET));

        SchemaManager schManager = mock(SchemaManager.class);

        when(schManager.schemaRegistry(anyLong(), anyInt())).thenReturn(completedFuture(null));

        String nodeName = "test";

        vaultManager = new VaultManager(new InMemoryVaultService());

        metaStorageManager = StandaloneMetaStorageManager.create(vaultManager, new SimpleInMemoryKeyValueStorage(nodeName));

        clockWaiter = new ClockWaiter(nodeName, clock);

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metaStorageManager), clockWaiter);

        indexManager = new IndexManager(
                tablesConfig,
                schManager,
                tableManagerMock,
                catalogManager,
                metaStorageManager,
                mock(Consumer.class)
        );

        vaultManager.start();
        metaStorageManager.start();
        clockWaiter.start();
        catalogManager.start();
        indexManager.start();

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        assertThat(
                catalogManager.createTable(
                        CreateTableParams.builder()
                                .schemaName(DEFAULT_SCHEMA_NAME)
                                .zone(DEFAULT_ZONE_NAME)
                                .tableName(TABLE_NAME)
                                .columns(List.of(
                                        ColumnParams.builder().name("c1").type(STRING).build(),
                                        ColumnParams.builder().name("c2").type(STRING).build()
                                ))
                                .colocationColumns(List.of("c1"))
                                .primaryKeyColumns(List.of("c1"))
                                .build()
                ),
                willCompleteSuccessfully()
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                vaultManager == null ? null : vaultManager::stop,
                metaStorageManager == null ? null : metaStorageManager::stop,
                clockWaiter == null ? null : clockWaiter::stop,
                catalogManager == null ? null : catalogManager::stop,
                indexManager == null ? null : indexManager::stop
        );
    }

    @Test
    void catalogChangedWhenCreateIsInvoked() {
        String indexName = "idx";

        assertThat(
                indexManager.createIndexAsync(
                        CreateSortedIndexParams.builder()
                                .schemaName(DEFAULT_SCHEMA_NAME)
                                .indexName(indexName)
                                .tableName(TABLE_NAME)
                                .columns(List.of("c1", "c2"))
                                .collations(List.of(ASC_NULLS_LAST, DESC_NULLS_FIRST))
                                .build(),
                        true
                ),
                willBe(true)
        );

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) catalogManager.index(indexName, clock.nowLong());
        CatalogTableDescriptor table = catalogManager.table(TABLE_NAME, clock.nowLong());

        assertEquals(table.id(), index.tableId());
        assertEquals(indexName, index.name());
        assertFalse(index.unique());

        assertThat(
                index.columns().stream().map(CatalogIndexColumnDescriptor::name).collect(toList()),
                contains("c1", "c2")
        );

        assertThat(
                index.columns().stream().map(CatalogIndexColumnDescriptor::collation).collect(toList()),
                contains(ASC_NULLS_LAST, DESC_NULLS_FIRST)
        );
    }

    @Test
    public void createIndexWithEmptyName() {
        assertThat(
                indexManager.createIndexAsync(
                        CreateHashIndexParams.builder()
                                .schemaName(DEFAULT_SCHEMA_NAME)
                                .indexName("")
                                .tableName("tName")
                                .build(),
                        true
                ),
                willThrowFast(IgniteInternalException.class, "Index name should be at least 1 character long")
        );
    }

    @Test
    public void dropNonExistingIndex() {
        String schemaName = DEFAULT_SCHEMA_NAME;
        String indexName = "nonExisting";

        assertThat(
                indexManager.dropIndexAsync(
                        DropIndexParams.builder().schemaName(schemaName).indexName(indexName).build(),
                        true
                ),
                willThrowFast(
                        IndexNotFoundException.class,
                        format("Index does not exist [name=\"{}\".\"{}\"]", schemaName, indexName)
                )
        );
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    // TODO: IGNITE-19500 может все поменяться и мне понадобиться идентификатор таблицы из конфигурации, скорее всего!
    public void eventIsFiredWhenIndexCreated() {
        String indexName = "idx";

        AtomicReference<IndexEventParameters> holder = new AtomicReference<>();

        indexManager.listen(IndexEvent.CREATE, (param, th) -> {
            holder.set(param);

            return completedFuture(true);
        });

        indexManager.listen(IndexEvent.DROP, (param, th) -> {
            holder.set(param);

            return completedFuture(true);
        });

        assertThat(
                indexManager.createIndexAsync(
                        CreateSortedIndexParams.builder()
                                .schemaName(DEFAULT_SCHEMA_NAME)
                                .indexName(indexName)
                                .tableName(TABLE_NAME)
                                .columns(List.of("c2"))
                                .collations(List.of(ASC_NULLS_LAST))
                                .build(),
                        true
                ),
                willBe(true)
        );

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) catalogManager.index(indexName, clock.nowLong());
        CatalogTableDescriptor table = catalogManager.table(TABLE_NAME, clock.nowLong());

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().indexId(), equalTo(index.id()));
        assertThat(holder.get().tableId(), equalTo(table.id()));
        assertThat(holder.get().indexDescriptor().name(), equalTo(indexName));

        assertThat(
                indexManager.dropIndexAsync(
                        DropIndexParams.builder().schemaName(DEFAULT_SCHEMA_NAME).indexName(indexName).build(),
                        true
                ),
                willBe(true)
        );

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().indexId(), equalTo(index.id()));
        assertThat(holder.get().tableId(), equalTo(table.id()));
    }
}
