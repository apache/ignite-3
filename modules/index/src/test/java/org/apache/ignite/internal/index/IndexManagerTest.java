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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.CatalogSchemaManager;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link IndexManager}.
 */
public class IndexManagerTest extends BaseIgniteAbstractTest {
    private static final String TABLE_NAME = "tName";

    private final HybridClock clock = new HybridClockImpl();

    private VaultManager vaultManager;

    private MetaStorageManager metaStorageManager;

    private ClockWaiter clockWaiter;

    private CatalogManager catalogManager;

    private IndexManager indexManager;

    @BeforeEach
    public void setUp() {
        TableManager tableManagerMock = mock(TableManager.class);

        when(tableManagerMock.tableAsync(anyLong(), anyInt())).thenAnswer(inv -> completedFuture(mockTable(inv.getArgument(1))));

        when(tableManagerMock.getTable(anyInt())).thenAnswer(inv -> mockTable(inv.getArgument(0)));

        when(tableManagerMock.localPartitionSetAsync(anyLong(), anyInt())).thenReturn(completedFuture(PartitionSet.EMPTY_SET));

        CatalogSchemaManager schManager = mock(CatalogSchemaManager.class);

        when(schManager.schemaRegistry(anyLong(), anyInt())).thenReturn(completedFuture(null));

        String nodeName = "test";

        vaultManager = new VaultManager(new InMemoryVaultService());

        metaStorageManager = StandaloneMetaStorageManager.create(vaultManager, new SimpleInMemoryKeyValueStorage(nodeName));

        clockWaiter = new ClockWaiter(nodeName, clock);

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metaStorageManager), clockWaiter);

        indexManager = new IndexManager(
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

        TableTestUtils.createTable(
                catalogManager,
                DEFAULT_SCHEMA_NAME,
                DEFAULT_ZONE_NAME,
                TABLE_NAME,
                List.of(
                        ColumnParams.builder().name("c1").type(STRING).build(),
                        ColumnParams.builder().name("c2").type(STRING).build()
                ),
                List.of("c1")
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(vaultManager, metaStorageManager, clockWaiter, catalogManager, indexManager);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
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
                catalogManager.execute(
                        CreateSortedIndexCommand.builder()
                                .schemaName(DEFAULT_SCHEMA_NAME)
                                .indexName(indexName)
                                .tableName(TABLE_NAME)
                                .columns(List.of("c2"))
                                .collations(List.of(ASC_NULLS_LAST))
                                .build()
                ),
                willBe(nullValue())
        );

        CatalogSortedIndexDescriptor index = (CatalogSortedIndexDescriptor) catalogManager.index(indexName, clock.nowLong());
        int tableId = index.tableId();

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().indexId(), equalTo(index.id()));
        assertThat(holder.get().tableId(), equalTo(tableId));
        assertThat(holder.get().indexDescriptor().name(), equalTo(indexName));

        assertThat(
                catalogManager.execute(DropIndexCommand.builder().schemaName(DEFAULT_SCHEMA_NAME).indexName(indexName).build()),
                willBe(nullValue())
        );

        assertThat(holder.get(), notNullValue());
        assertThat(holder.get().indexId(), equalTo(index.id()));
        assertThat(holder.get().tableId(), equalTo(tableId));
    }

    private static TableImpl mockTable(int tableId) {
        InternalTable internalTable = mock(InternalTable.class);

        when(internalTable.tableId()).thenReturn(tableId);

        return new TableImpl(internalTable, new HeapLockManager());
    }
}
