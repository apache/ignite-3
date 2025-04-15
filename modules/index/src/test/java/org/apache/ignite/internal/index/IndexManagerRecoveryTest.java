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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleIndex;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleTable;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.table.QualifiedName.DEFAULT_SCHEMA_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link IndexManager} recovery.
 */
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(MockitoExtension.class)
public class IndexManagerRecoveryTest extends BaseIgniteAbstractTest {
    private MetaStorageManager metaStorageManager;

    private CatalogManager catalogManager;

    private IndexManager indexManager;

    @Mock
    private TableManager mockTableManager;

    @Mock
    private TableViewInternal mockTable;

    private final LowWatermark lowWatermark = new TestLowWatermark();

    private final HybridClockImpl clock = new HybridClockImpl();

    @BeforeEach
    void setUp(
            @Mock SchemaManager mockSchemaManager,
            @InjectExecutorService ExecutorService executorService
    ) {
        when(mockTableManager.cachedTable(anyInt())).thenReturn(mockTable);

        String nodeName = "test-node";

        metaStorageManager = StandaloneMetaStorageManager.create(nodeName, clock);

        catalogManager = createTestCatalogManager(nodeName, clock, metaStorageManager);

        indexManager = new IndexManager(
                mockSchemaManager,
                mockTableManager,
                catalogManager,
                executorService,
                new MetaStorageRevisionListenerRegistry(metaStorageManager),
                lowWatermark
        );

        var componentContext = new ComponentContext();

        CompletableFuture<Void> startFuture = metaStorageManager.startAsync(componentContext)
                .thenCompose(v -> metaStorageManager.recoveryFinishedFuture())
                .thenCompose(v -> catalogManager.startAsync(componentContext))
                .thenCompose(v -> metaStorageManager.deployWatches())
                .thenCompose(v -> catalogManager.catalogInitializationFuture());

        assertThat(startFuture, willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        List<IgniteComponent> components = List.of(indexManager, catalogManager, metaStorageManager);

        components.forEach(IgniteComponent::beforeNodeStop);

        stopAsync(new ComponentContext(), components);
    }

    @Test
    void indicesAreScheduledForRemovalOnRecovery() {
        String tableName1 = "table1";
        String indexName1 = "index1";

        String tableName2 = "table2";
        String indexName2 = "index2";

        createSimpleTable(catalogManager, tableName1);
        createSimpleHashIndex(catalogManager, tableName1, indexName1);

        createSimpleTable(catalogManager, tableName2);
        createSimpleHashIndex(catalogManager, tableName2, indexName2);

        int indexId1 = indexId(indexName1);
        int indexId2 = indexId(indexName2);

        // Drop the index, but not the table.
        dropSimpleIndex(catalogManager, indexName1);

        // Drop the table, but not the index.
        dropSimpleTable(catalogManager, tableName2);

        assertThat(startAsync(new ComponentContext(), indexManager), willCompleteSuccessfully());

        verify(mockTable, never()).unregisterIndex(indexId1);
        verify(mockTable, never()).unregisterIndex(indexId2);

        lowWatermark.updateLowWatermark(clock.now());

        verify(mockTable, timeout(1000)).unregisterIndex(indexId1);
        verify(mockTable, never()).unregisterIndex(indexId2);
    }

    @Test
    void indicesAreNotScheduledForRemovalOnRecoveryWhenTableGetsDeleted() {
        String tableName1 = "table1";
        String indexName1 = "index1";

        String tableName2 = "table2";
        String indexName2 = "index2";

        createSimpleTable(catalogManager, tableName1);
        createSimpleHashIndex(catalogManager, tableName1, indexName1);

        createSimpleTable(catalogManager, tableName2);
        createSimpleHashIndex(catalogManager, tableName2, indexName2);

        int indexId1 = indexId(indexName1);
        int indexId2 = indexId(indexName2);

        // Drop the index, but not the table.
        dropSimpleIndex(catalogManager, indexName1);

        // Drop the table, but not the index.
        dropSimpleTable(catalogManager, tableName2);

        assertThat(startAsync(new ComponentContext(), indexManager), willCompleteSuccessfully());

        verify(mockTable, never()).unregisterIndex(indexId1);
        verify(mockTable, never()).unregisterIndex(indexId2);

        lowWatermark.updateLowWatermark(clock.now());

        verify(mockTable, timeout(1000)).unregisterIndex(indexId1);
        verify(mockTable, never()).unregisterIndex(indexId2);
    }

    private int indexId(String indexName) {
        return catalogManager.activeCatalog(clock.nowLong())
                .aliveIndex(DEFAULT_SCHEMA_NAME, indexName)
                .id();
    }
}
