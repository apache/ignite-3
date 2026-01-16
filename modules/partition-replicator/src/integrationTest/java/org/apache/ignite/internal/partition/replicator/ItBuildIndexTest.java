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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.sql.ColumnType.DOUBLE;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.index.message.IndexMessageGroup;
import org.apache.ignite.internal.index.message.IndexMessagesFactory;
import org.apache.ignite.internal.index.message.IsNodeFinishedRwTransactionsStartedBeforeRequest;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
/**
 * Tests building indices for colocation track.
 */
@Timeout(60)
public class ItBuildIndexTest extends ItAbstractColocationTest {
    private static final IndexMessagesFactory FACTORY = new IndexMessagesFactory();

    @Test
    public void testBuildIndex() throws Exception {
        // Prepare a single node cluster.
        startCluster(1);
        Node node = getNode(0);

        String zoneName = "test-zone";
        createZone(node, zoneName, 1, 1);

        String tableName = "TEST_TABLE";
        createCustomTable(node, zoneName, tableName);
        int tableId = TableTestUtils.getTableId(node.catalogManager, tableName, node.hybridClock.nowLong());

        // Test node does not create IndexNodeFinishedRwTransactionsChecker, so the following code is needed to unblock index building.
        // It's easier than creating a real service with all its dependencies.
        node.clusterService.messagingService().addMessageHandler(
                IndexMessageGroup.class,
                (message, sender, correlationId) -> {
                    if (message instanceof IsNodeFinishedRwTransactionsStartedBeforeRequest) {
                        node.clusterService.messagingService().respond(
                                sender,
                                FACTORY.isNodeFinishedRwTransactionsStartedBeforeResponse().finished(true).build(),
                                correlationId
                        );
                    }
                });

        TableViewInternal tableViewInternal = node.tableManager.table(tableId);
        KeyValueView<Tuple, Tuple> tableView = tableViewInternal.keyValueView();

        node.transactions().runInTransaction(tx -> {
            Tuple key = Tuple.create().set("KEY", 1L);
            Tuple value = Tuple.create().set("VAL", 1).set("DOUBLEVAL", 1.0);
            tableView.putAll(tx, Map.of(key, value));
        });

        node.sql().execute("CREATE INDEX TEST_INDEX ON TEST_TABLE (DOUBLEVAL)").close();

        assertTrue(waitForCondition(() -> isIndexAvailable(node, "TEST_INDEX"), 10_000L));
    }

    private static void createCustomTable(Node node, String zoneName, String tableName) {
        node.waitForMetadataCompletenessAtNow();

        TableTestUtils.createTable(
                node.catalogManager,
                DEFAULT_SCHEMA_NAME,
                zoneName,
                tableName,
                List.of(
                        ColumnParams.builder().name("KEY").type(INT64).build(),
                        ColumnParams.builder().name("VAL").type(INT32).nullable(true).build(),
                        ColumnParams.builder().name("DOUBLEVAL").type(DOUBLE).build()
                ),
                List.of("KEY")
        );
    }

    private static boolean isIndexAvailable(Node node, String indexName) {
        CatalogManager catalogManager = node.catalogManager;
        HybridClock clock = node.hybridClock;

        CatalogIndexDescriptor indexDescriptor = catalogManager
                .activeCatalog(clock.nowLong())
                .aliveIndex(DEFAULT_SCHEMA_NAME, indexName);

        return indexDescriptor != null && indexDescriptor.status() == AVAILABLE;
    }
}
