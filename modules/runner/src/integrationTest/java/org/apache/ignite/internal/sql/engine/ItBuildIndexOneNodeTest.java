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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.table.TableTestUtils.getIndexStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Integration test for testing the building of an index in a single node cluster. */
public class ItBuildIndexOneNodeTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = zoneName(TABLE_NAME);

    private static final String INDEX_NAME = "TEST_INDEX";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);

        CLUSTER.runningNodes().forEach(IgniteImpl::stopDroppingMessages);
    }

    @Test
    void testRecoveryBuildingIndex() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPersons(TABLE_NAME, new Person(0, "0", 10.0));

        CompletableFuture<Void> awaitBuildIndexReplicaRequest = new CompletableFuture<>();

        CLUSTER.node(0).dropMessages((s, networkMessage) -> {
            if (networkMessage instanceof BuildIndexReplicaRequest) {
                awaitBuildIndexReplicaRequest.complete(null);

                return true;
            }

            return false;
        });

        createIndex(TABLE_NAME, INDEX_NAME, "ID");

        assertThat(awaitBuildIndexReplicaRequest, willCompleteSuccessfully());

        // Let's restart the node.
        CLUSTER.stopNode(0);
        CLUSTER.startNode(0);

        awaitIndexBuildingComplete(CLUSTER.node(0), INDEX_NAME);
    }

    // TODO: IGNITE-20638 It will be enough for us to wait for the index to be available in the catalog
    private static void awaitIndexBuildingComplete(IgniteImpl ignite, String indexName) throws Exception {
        CatalogManager catalogManager = ignite.catalogManager();
        HybridClock clock = ignite.clock();

        CatalogIndexDescriptor indexDescriptor = getIndexStrict(catalogManager, indexName, clock.nowLong());
        CatalogTableDescriptor tableDescriptor = getTableStrict(catalogManager, indexDescriptor.tableId(), clock.nowLong());

        TableViewInternal tableImpl = getTableView(ignite, tableDescriptor.name());

        assertNotNull(tableImpl, tableDescriptor.name());

        for (int partitionId = 0; partitionId < tableImpl.internalTable().partitions(); partitionId++) {
            int finalPartitionId = partitionId;

            assertTrue(waitForCondition(() -> {
                IndexStorage indexStorage = tableImpl.internalTable().storage().getIndex(finalPartitionId, indexDescriptor.id());

                return indexStorage != null && indexStorage.getNextRowIdToBuild() == null;
            }, 100, 5_000), "indexName=" + indexName + ", partitionId=" + partitionId);
        }
    }
}
