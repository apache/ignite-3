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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.network.NetworkAddress;

/** Helper class for testing index management. */
class TestIndexManagementUtils {
    static final String NODE_NAME = "test-node";

    static final UUID NODE_ID = new UUID(1, 2);

    static final String TABLE_NAME = "test-table";

    static final String COLUMN_NAME = "test-column";

    static final String INDEX_NAME = "test-index";

    static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    static final InternalClusterNode LOCAL_NODE = new ClusterNodeImpl(NODE_ID, NODE_NAME, new NetworkAddress("127.0.0.1", 8888));

    static final LogicalNode LOGICAL_LOCAL_NODE = new LogicalNode(NODE_ID, NODE_NAME, new NetworkAddress("127.0.0.1", 8888));

    static void createTable(CatalogManager catalogManager, String tableName, String columnName) {
        TableTestUtils.createTable(
                catalogManager,
                SqlCommon.DEFAULT_SCHEMA_NAME,
                null,
                tableName,
                List.of(ColumnParams.builder().name(columnName).type(INT32).build()),
                List.of(columnName)
        );
    }

    static void createIndex(CatalogManager catalogManager, String tableName, String indexName, String columnName) {
        TableTestUtils.createHashIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, tableName, indexName, List.of(columnName), false);
    }

    static void dropIndex(CatalogManager catalogManager, String indexName) {
        TableTestUtils.dropIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, indexName);
    }

    static int indexId(CatalogService catalogService, String indexName, HybridClock clock) {
        return TableTestUtils.getIndexIdStrict(catalogService, indexName, clock.nowLong());
    }

    static CatalogIndexDescriptor indexDescriptor(CatalogService catalogService, String indexName, HybridClock clock) {
        return TableTestUtils.getIndexStrict(catalogService, indexName, clock.nowLong());
    }

    static int tableId(CatalogService catalogService, String tableName, HybridClock clock) {
        return TableTestUtils.getTableIdStrict(catalogService, tableName, clock.nowLong());
    }

    static void makeIndexAvailable(CatalogManager catalogManager, int indexId) {
        assertThat(catalogManager.execute(MakeIndexAvailableCommand.builder().indexId(indexId).build()), willCompleteSuccessfully());
    }

    static void awaitTillGlobalMetastoreRevisionIsApplied(MetaStorageManagerImpl metaStorageManager) throws Exception {
        assertTrue(
                waitForCondition(() -> {
                    CompletableFuture<Long> currentRevisionFuture = metaStorageManager.currentRevision();

                    assertThat(currentRevisionFuture, willCompleteSuccessfully());

                    return currentRevisionFuture.join() == metaStorageManager.appliedRevision();
                }, 1_000)
        );
    }

    static void assertMetastoreKeyAbsent(MetaStorageManager metaStorageManager, ByteArray key) {
        assertThat(metaStorageManager.get(key).thenApply(Entry::value), willBe(nullValue()));
    }

    static void assertMetastoreKeyPresent(MetaStorageManager metaStorageManager, ByteArray key) {
        assertThat(metaStorageManager.get(key).thenApply(Entry::value), willBe(notNullValue()));
    }

    static ReplicaMeta newPrimaryReplicaMeta(
            InternalClusterNode clusterNode,
            ReplicationGroupId replicaGroupId,
            HybridTimestamp startTime,
            HybridTimestamp expirationTime
    ) {
        return new Lease(clusterNode.name(), clusterNode.id(), startTime, expirationTime, replicaGroupId);
    }

    static boolean isIndexAvailable(CatalogService catalogService, String indexName, HybridClock clock) {
        return TableTestUtils.getIndexStrict(catalogService, indexName, clock.nowLong()).status() == AVAILABLE;
    }

    static void startBuildingIndex(CatalogManager catalogManager, int indexId) {
        assertThat(catalogManager.execute(StartBuildingIndexCommand.builder().indexId(indexId).build()), willCompleteSuccessfully());
    }
}
