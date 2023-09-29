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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.table.TableTestUtils.createHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createTable;
import static org.apache.ignite.internal.table.TableTestUtils.dropTable;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.CatalogSchemaManager;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
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

    private static final String COLUMN_NAME = "c";

    private final HybridClock clock = new HybridClockImpl();

    private VaultManager vaultManager;

    private MetaStorageManagerImpl metaStorageManager;

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
                (LongFunction<CompletableFuture<?>> function) -> metaStorageManager.registerRevisionUpdateListener(function::apply)
        );

        List.of(vaultManager, metaStorageManager, clockWaiter, catalogManager, indexManager).forEach(IgniteComponent::start);

        assertThat(metaStorageManager.recoveryFinishedFuture(), willCompleteSuccessfully());
        assertThat(metaStorageManager.notifyRevisionUpdateListenerOnStart(), willCompleteSuccessfully());
        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        createTable(
                catalogManager,
                DEFAULT_SCHEMA_NAME,
                DEFAULT_ZONE_NAME,
                TABLE_NAME,
                List.of(ColumnParams.builder().name(COLUMN_NAME).length(100).type(STRING).build()),
                List.of(COLUMN_NAME)
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(vaultManager, metaStorageManager, clockWaiter, catalogManager, indexManager);
    }

    @Test
    void testGetMvTableStorageForNonExistsTable() {
        assertThat(getMvTableStorageLatestRevision(Integer.MAX_VALUE), willBe(nullValue()));
    }

    @Test
    void testGetMvTableStorageForExistsTable() {
        assertThat(getMvTableStorageLatestRevision(tableId()), willBe(notNullValue()));
    }

    @Test
    void testGetMvTableStorageForDroppedTable() {
        dropTable(catalogManager, DEFAULT_SCHEMA_NAME, TABLE_NAME);

        assertThat(getMvTableStorageLatestRevision(Integer.MAX_VALUE), willBe(nullValue()));
    }

    @Test
    void testGetMvTableStorageForNewIndexInCatalogListener() {
        CompletableFuture<MvTableStorage> getMvTableStorageInCatalogListenerFuture = new CompletableFuture<>();

        catalogManager.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                getMvTableStorageInCatalogListenerFuture.completeExceptionally(exception);
            } else {
                try {
                    CompletableFuture<MvTableStorage> mvTableStorageFuture = getMvTableStorage(parameters.causalityToken(), tableId());

                    assertFalse(mvTableStorageFuture.isDone());

                    mvTableStorageFuture.whenComplete((mvTableStorage, throwable) -> {
                        if (throwable != null) {
                            getMvTableStorageInCatalogListenerFuture.completeExceptionally(throwable);
                        } else {
                            getMvTableStorageInCatalogListenerFuture.complete(mvTableStorage);
                        }
                    });
                } catch (Throwable t) {
                    getMvTableStorageInCatalogListenerFuture.completeExceptionally(t);
                }
            }

            return completedFuture(false);
        });

        createHashIndex(
                catalogManager,
                DEFAULT_SCHEMA_NAME,
                TABLE_NAME,
                TABLE_NAME + "_test_index",
                List.of(COLUMN_NAME),
                false
        );

        assertThat(getMvTableStorageInCatalogListenerFuture, willBe(notNullValue()));
    }

    private TableImpl mockTable(int tableId) {
        CatalogZoneDescriptor zone = catalogManager.zone(DEFAULT_ZONE_NAME, clock.nowLong());

        assertNotNull(zone);

        StorageTableDescriptor storageTableDescriptor = new StorageTableDescriptor(tableId, zone.partitions(), DEFAULT_DATA_REGION);

        MvTableStorage mvTableStorage = mock(MvTableStorage.class);

        when(mvTableStorage.getTableDescriptor()).thenReturn(storageTableDescriptor);

        InternalTable internalTable = mock(InternalTable.class);

        when(internalTable.tableId()).thenReturn(tableId);
        when(internalTable.storage()).thenReturn(mvTableStorage);

        return new TableImpl(internalTable, new HeapLockManager());
    }

    private CompletableFuture<MvTableStorage> getMvTableStorageLatestRevision(int tableId) {
        return metaStorageManager.getService().currentRevision().thenCompose(latestRevision -> getMvTableStorage(latestRevision, tableId));
    }

    private CompletableFuture<MvTableStorage> getMvTableStorage(long causalityToken, int tableId) {
        return indexManager.getMvTableStorage(causalityToken, tableId);
    }

    private int tableId() {
        return getTableIdStrict(catalogManager, TABLE_NAME, clock.nowLong());
    }
}
