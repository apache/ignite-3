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

package org.apache.ignite.internal.table.distributed.index;

import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toIndexDescriptor;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toTableDescriptor;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findIndexView;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findTableView;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.HashIndexChange;
import org.apache.ignite.internal.schema.configuration.index.TableIndexChange;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/** For {@link IndexBuildController} testing. */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class IndexBuildControllerTest {
    private static final String TABLE_NAME_0 = "test_table_0";

    private static final String TABLE_NAME_1 = "test_table_1";

    private static final String INDEX_NAME_0 = "test_index_table_0_index_0";

    private static final String INDEX_NAME_1 = "test_index_table_1_index_0";

    @InjectConfiguration
    private TablesConfiguration tablesConfig;

    @Mock
    private IndexBuilder indexBuilder;

    @Mock
    private RaftGroupService raftClient;

    private IndexBuildController controller;

    @BeforeEach
    void setUp() {
        CompletableFuture<Void> changeConfigFuture = tablesConfig.change(tablesChange -> {
            tablesChange.changeTables().create(TABLE_NAME_0, tableChange -> simpleTable(tableChange, 1));
            tablesChange.changeTables().create(TABLE_NAME_1, tableChange -> simpleTable(tableChange, 2));

            tablesChange.changeIndexes().create(INDEX_NAME_0, indexChange -> simpleIndex(indexChange, 1, 3));
            tablesChange.changeIndexes().create(INDEX_NAME_1, indexChange -> simpleIndex(indexChange, 2, 5));
        });

        assertThat(changeConfigFuture, willCompleteSuccessfully());

        controller = new IndexBuildController(indexBuilder, tablesConfig);
    }

    @Test
    void testOnIndexCreate() {
        int tableId0 = tableId(TABLE_NAME_0);
        int tableId1 = tableId(TABLE_NAME_1);

        controller.onBecomePrimary(replicaId(tableId0, 0), tableStorage(0), raftClient);
        controller.onBecomePrimary(replicaId(tableId0, 1), tableStorage(1), raftClient);

        controller.onBecomePrimary(replicaId(tableId1, 2), tableStorage(2), raftClient);
        controller.onBecomePrimary(replicaId(tableId1, 3), tableStorage(3), raftClient);

        // Let's check that the index build was started only for the first two primary replicas.
        clearInvocations(indexBuilder);

        int indexId0 = indexId(INDEX_NAME_0);

        controller.onIndexCreate(tableDescriptor(tableId0), indexDescriptor(indexId0));

        verify(indexBuilder).startBuildIndex(eq(tableId0), eq(0), eq(indexId0), any(), any(), eq(raftClient));
        verify(indexBuilder).startBuildIndex(eq(tableId0), eq(1), eq(indexId0), any(), any(), eq(raftClient));

        verify(indexBuilder, never()).startBuildIndex(anyInt(), eq(2), anyInt(), any(), any(), eq(raftClient));
        verify(indexBuilder, never()).startBuildIndex(anyInt(), eq(3), anyInt(), any(), any(), eq(raftClient));

        // Let's check that only for the rest of the replicas we start the index build.
        clearInvocations(indexBuilder);

        int indexId1 = indexId(INDEX_NAME_1);

        controller.onIndexCreate(tableDescriptor(tableId1), indexDescriptor(indexId1));

        verify(indexBuilder, never()).startBuildIndex(anyInt(), eq(0), anyInt(), any(), any(), eq(raftClient));
        verify(indexBuilder, never()).startBuildIndex(anyInt(), eq(1), anyInt(), any(), any(), eq(raftClient));

        verify(indexBuilder).startBuildIndex(eq(tableId1), eq(2), eq(indexId1), any(), any(), eq(raftClient));
        verify(indexBuilder).startBuildIndex(eq(tableId1), eq(3), eq(indexId1), any(), any(), eq(raftClient));
    }

    @Test
    void testOnIndexDestroy() {
        int tableId0 = tableId(TABLE_NAME_0);
        int tableId1 = tableId(TABLE_NAME_1);

        controller.onBecomePrimary(replicaId(tableId0, 0), tableStorage(0), raftClient);
        controller.onBecomePrimary(replicaId(tableId0, 1), tableStorage(1), raftClient);

        controller.onBecomePrimary(replicaId(tableId1, 2), tableStorage(2), raftClient);
        controller.onBecomePrimary(replicaId(tableId1, 3), tableStorage(3), raftClient);

        // Let's check that the index build was stopped only for the first two primary replicas.
        clearInvocations(indexBuilder);

        int indexId0 = indexId(INDEX_NAME_0);

        controller.onIndexDestroy(tableDescriptor(tableId0), indexDescriptor(indexId0));

        verify(indexBuilder).stopBuildIndex(eq(tableId0), eq(0), eq(indexId0));
        verify(indexBuilder).stopBuildIndex(eq(tableId0), eq(1), eq(indexId0));

        verify(indexBuilder, never()).stopBuildIndex(anyInt(), eq(2), anyInt());
        verify(indexBuilder, never()).stopBuildIndex(anyInt(), eq(3), anyInt());

        // Let's check that only for the rest of the replicas we stop the index build.
        clearInvocations(indexBuilder);

        int indexId1 = indexId(INDEX_NAME_1);

        controller.onIndexDestroy(tableDescriptor(tableId1), indexDescriptor(indexId1));

        verify(indexBuilder, never()).stopBuildIndex(anyInt(), eq(0), anyInt());
        verify(indexBuilder, never()).stopBuildIndex(anyInt(), eq(1), anyInt());

        verify(indexBuilder).stopBuildIndex(eq(tableId1), eq(2), eq(indexId1));
        verify(indexBuilder).stopBuildIndex(eq(tableId1), eq(3), eq(indexId1));
    }

    @Test
    void testOnBecomePrimary() {
        int tableId = tableId(TABLE_NAME_0);

        controller.onBecomePrimary(replicaId(tableId, 0), tableStorage(0), raftClient);

        int indexId = indexId(INDEX_NAME_0);

        verify(indexBuilder).startBuildIndex(eq(tableId), eq(0), eq(indexId), any(), any(), eq(raftClient));
    }

    @Test
    void testOnStopBeingPrimary() {
        int tableId = tableId(TABLE_NAME_0);

        controller.onBecomePrimary(replicaId(tableId, 0), tableStorage(0), raftClient);

        controller.onStopBeingPrimary(replicaId(tableId, 0));

        verify(indexBuilder).stopBuildIndexes(eq(tableId), eq(0));
    }

    private CatalogTableDescriptor tableDescriptor(int tableId) {
        return toTableDescriptor(findTableView(tablesConfig.value(), tableId));
    }

    private CatalogIndexDescriptor indexDescriptor(int indexId) {
        return toIndexDescriptor(findIndexView(tablesConfig.value(), indexId));
    }

    private static MvTableStorage tableStorage(int partitionId) {
        MvTableStorage tableStorage = mock(MvTableStorage.class);

        when(tableStorage.getMvPartition(partitionId)).thenReturn(mock(MvPartitionStorage.class));

        when(tableStorage.getOrCreateIndex(eq(partitionId), any())).thenReturn(mock(IndexStorage.class));

        return tableStorage;
    }

    private static TablePartitionId replicaId(int tableId, int partitionId) {
        return new TablePartitionId(tableId, partitionId);
    }

    private static void simpleTable(TableChange tableChange, int tableId) {
        ((ExtendedTableChange) tableChange)
                .changeSchemaId(1)
                .changeId(tableId)
                .changePrimaryKey(primaryKeyChange -> primaryKeyChange.changeColumns("key").changeColocationColumns("key"))
                .changeColumns().create("key", columnChange -> columnChange.changeType().changeType("STRING"));
    }

    private static void simpleIndex(TableIndexChange indexChange, int tableId, int indexId) {
        indexChange
                .convert(HashIndexChange.class)
                .changeColumnNames("key")
                .changeId(indexId)
                .changeTableId(tableId);
    }

    private int tableId(String tableName) {
        return tablesConfig.tables().get(tableName).id().value();
    }

    private int indexId(String indexName) {
        return tablesConfig.indexes().get(indexName).id().value();
    }
}
