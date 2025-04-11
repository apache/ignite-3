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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTransaction;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication

/**
 * Tests transactions vacuum for colocation track.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "500")
@Timeout(60)
public class ItTransactionsVacuumTest extends ItAbstractColocationTest {
    /**
     * Tests transactions vacuum.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionsVacuum(
            @InjectConfiguration("mock.properties.txnResourceTtl=\"50\"") SystemDistributedConfiguration systemCfg) throws Exception {
        this.systemDistributedConfiguration = systemCfg;

        startCluster(1);
        Node node = cluster.get(0);

        String zoneName = "test-zone";
        createZone(node, zoneName, 1, 1);
        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, zoneName, node.hybridClock.nowLong());

        String tableName = "test_table";
        createTable(node, zoneName, tableName);

        int tableId = TableTestUtils.getTableId(node.catalogManager, tableName, node.hybridClock.nowLong());
        TableViewInternal tableViewInternal = node.tableManager.table(tableId);
        KeyValueView<Long, Integer> tableView = tableViewInternal.keyValueView(Long.class, Integer.class);

        Transaction tx = node.transactions().begin();
        UUID txId = txId(tx);

        tableView.putAll(tx, Map.of(0L, 0, 1L, 1));

        assertNotNull(volatileTxState(node, txId), "Volatile TX state is absent.");

        assertNull(persistentTxState(node, zoneId, 0, txId), "Persistent TX state exists for non-completed TX.");

        tx.commit();

        // Check that volatile tx state is removed.
        assertTrue(waitForCondition(() -> volatileTxState(node, txId) == null, 10_000));

        // Check that persistent tx state is removed.
        assertTrue(waitForCondition(() -> persistentTxState(node, zoneId, 0, txId) == null, 10_000));
    }

    /**
     * Returns transaction's meta from volatile storage.
     *
     * @param node Node.
     * @param txId Transaction id.
     * @return Transaction meta.
     */
    private static TransactionMeta volatileTxState(Node node, UUID txId) {
        return node.txManager().stateMeta(txId);
    }

    /**
     * Returns transaction's meta from persistent storage.
     *
     * @param node Node.
     * @param zoneId Zone id.
     * @param partId Partition id.
     * @param txId Transaction id.
     * @return Transaction meta.
     */
    private static TransactionMeta persistentTxState(Node node, int zoneId, int partId, UUID txId) {
        return IgniteTestUtils.bypassingThreadAssertions(() -> requireNonNull(node.txStatePartitionStorage(zoneId, partId)).get(txId));
    }

    /**
     * Returns transaction identifier for the given transaction.
     *
     * @param tx Transaction.
     * @return Transaction id.
     */
    private static UUID txId(Transaction tx) {
        return ((ReadWriteTransactionImpl) unwrapIgniteTransaction(tx)).id();
    }
}
