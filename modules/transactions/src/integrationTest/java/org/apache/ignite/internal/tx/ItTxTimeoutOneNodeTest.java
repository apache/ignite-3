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

package org.apache.ignite.internal.tx;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getDefaultZone;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

abstract class ItTxTimeoutOneNodeTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST";

    @Override
    protected int initialNodes() {
        return 1;
    }

    abstract Ignite ignite();

    abstract InternalTransaction toInternalTransaction(Transaction tx);

    private Table createTestTable() throws InterruptedException {
        if (colocationEnabled()) {
            IgniteImpl node = unwrapIgniteImpl(node(0));
            CatalogManager catalogManager = node.catalogManager();
            CatalogZoneDescriptor defaultZone = getDefaultZone(catalogManager, node.clock().nowLong());

            // Generally it's required to await default zone dataNodesAutoAdjustScaleUp timeout in order to treat zone as ready one.
            // In order to eliminate awaiting interval, default zone scaleUp is altered to be immediate.
            node(0).sql().executeScript(String.format("ALTER ZONE \"%s\"SET (AUTO SCALE UP 0)", defaultZone.name()));
        }

        ignite().sql().executeScript("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR)");

        // This test is rather fragile because it's time dependent. The test uses one second as tx timeout and assumes that it's enough
        // for an initial operation to find the primary replica, which might not be the case in case of concurrent interleaving rebalance.
        // Not related to colocation. awaitAssignmentsStabilization awaits that the default zone/table stable partition assignments size
        // will be DEFAULT_PARTITION_COUNT * DEFAULT_REPLICA_COUNT. It's correct only for a single-node cluster that uses default zone,
        // that's why given method isn't located in a utility class.
        awaitAssignmentsStabilization(cluster.node(0));

        return ignite().tables().table(TABLE_NAME);
    }

    @Test
    void roTransactionTimesOut() throws Exception {
        Table table = createTestTable();

        Transaction roTx = ignite().transactions().begin(new TransactionOptions().readOnly(true).timeoutMillis(100));

        // Make sure the RO tx actually begins on the server (as thin client transactions are lazy).
        doGetOn(table, roTx);

        assertTrue(
                waitForCondition(() -> toInternalTransaction(roTx).isFinishingOrFinished(), SECONDS.toMillis(10)),
                "Transaction should have been finished due to timeout"
        );

        assertThrows(TransactionException.class, () -> doGetOn(table, roTx));
        // TODO: uncomment the following assert after IGNITE-24233 is fixed.
        // assertThrows(TransactionException.class, roTx::commit);
    }

    @Test
    void readWriteTransactionTimesOut() throws InterruptedException {
        Table table = createTestTable();

        Transaction rwTx = ignite().transactions().begin(new TransactionOptions().readOnly(false).timeoutMillis(1_000));

        // Make sure the tx actually begins on the server (as thin client transactions are lazy).
        doPutOn(table, rwTx);

        assertTrue(
                waitForCondition(() -> toInternalTransaction(rwTx).isFinishingOrFinished(), SECONDS.toMillis(10)),
                "Transaction should have been finished due to timeout"
        );

        assertThrows(TransactionException.class, () -> doGetOn(table, rwTx));
        // TODO: uncomment the following assert after IGNITE-24233 is fixed.
        // assertThrows(TransactionException.class, roTx::commit);
    }

    @Test
    void timeoutExceptionHasCorrectCause() throws InterruptedException {
        Table table = createTestTable();

        Transaction rwTx = ignite().transactions().begin(new TransactionOptions().readOnly(false).timeoutMillis(1_000));

        // Wait for an exception.
        assertTrue(
                waitForCondition(() -> timeoutExceeded(table, rwTx), 1_000, 10_000),
                "Write operation should throw an exception with TX_ALREADY_FINISHED_WITH_TIMEOUT_ER error code"
        );

        assertThrows(TransactionException.class, () -> doGetOn(table, rwTx));
    }

    private static boolean timeoutExceeded(Table table, Transaction rwTx) {
        try {
            doPutOn(table, rwTx);
            return false;
        } catch (TransactionException ex) {
            if (ex.code() == Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR) {
                return true;
            } else {
                fail("Expected exception code to be TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR but found: " + ex.getMessage());
                return false;
            }
        }
    }

    private static void doGetOn(Table table, Transaction tx) {
        table.keyValueView(Integer.class, String.class).get(tx, 1);
    }

    private static void doPutOn(Table table, Transaction tx) {
        table.keyValueView(Integer.class, String.class).put(tx, 1, "one");
    }

    private static void awaitAssignmentsStabilization(Ignite node) throws InterruptedException {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);
        TableImpl table = unwrapTableImpl(node.tables().table(TABLE_NAME));
        int tableOrZoneId = colocationEnabled() ? table.zoneId() : table.tableId();

        HybridTimestamp timestamp = igniteImpl.clock().now();

        assertTrue(waitForCondition(() -> {
            int totalPartitionSize = 0;

            // Within given test, default zone is used.
            for (int p = 0; p < DEFAULT_PARTITION_COUNT; p++) {
                CompletableFuture<TokenizedAssignments> assignmentsFuture = igniteImpl.placementDriver().getAssignments(
                        colocationEnabled()
                                ? new ZonePartitionId(tableOrZoneId, p)
                                : new TablePartitionId(tableOrZoneId, p),
                        timestamp);

                assertThat(assignmentsFuture, willCompleteSuccessfully());

                totalPartitionSize += assignmentsFuture.join().nodes().size();
            }

            return totalPartitionSize == DEFAULT_PARTITION_COUNT * DEFAULT_REPLICA_COUNT;
        }, 10_000));
    }
}
