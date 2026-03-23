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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTransactionsImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxPriority;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.views.TransactionsViewProvider;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code TRANSACTIONS} system view.
 */
public class ItTransactionsSystemViewTest extends AbstractSystemViewTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    public void testMetadata() {
        assertQuery("SELECT * FROM SYSTEM.TRANSACTIONS")
                .columnMetadata(
                        new MetadataMatcher().name("COORDINATOR_NODE_ID").type(ColumnType.STRING).nullable(false),
                        new MetadataMatcher().name("TRANSACTION_STATE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TRANSACTION_ID").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TRANSACTION_START_TIME").type(ColumnType.TIMESTAMP).nullable(true),
                        new MetadataMatcher().name("TRANSACTION_TYPE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TRANSACTION_PRIORITY").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TRANSACTION_LABEL").type(ColumnType.STRING).nullable(true),

                        // Legacy columns.
                        new MetadataMatcher().name("STATE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ID").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("START_TIME").type(ColumnType.TIMESTAMP).nullable(true),
                        new MetadataMatcher().name("TYPE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("PRIORITY").type(ColumnType.STRING).nullable(true)
                )
                .check();
    }

    @Test
    public void testData() {
        Map<UUID, String> nodeIdToName = new HashMap<>();
        List<Transaction> txs = new ArrayList<>();

        CLUSTER.runningNodes().forEach(node -> {
            IgniteTransactionsImpl transactions = unwrapIgniteTransactionsImpl(node.transactions());

            for (TxPriority priority : TxPriority.values()) {
                txs.add(transactions.beginWithPriority(true, priority));
                txs.add(transactions.beginWithPriority(false, priority));
            }

            InternalClusterNode localMember = unwrapIgniteImpl(node).clusterService().topologyService().localMember();

            nodeIdToName.put(localMember.id(), localMember.name());
        });

        // Verify rows count.
        assertQuery("SELECT count(*) FROM SYSTEM.TRANSACTIONS")
                .returns((long) txs.size() + /* implicit tx used for query */ 1)
                .check();

        // Verify view data for each transaction.
        for (Transaction tx0 : txs) {
            InternalTransaction tx = (InternalTransaction) tx0;

            assertQuery("SELECT COORDINATOR_NODE_ID, TRANSACTION_STATE, TRANSACTION_ID, TRANSACTION_START_TIME, TRANSACTION_TYPE, "
                    + "TRANSACTION_PRIORITY FROM SYSTEM.TRANSACTIONS WHERE ID = '" + tx.id() + "'")
                    .returns(makeExpectedRow(tx, nodeIdToName))
                    .check();
        }

        // Completing all explicitly started transactions.
        for (int i = 0; i < txs.size(); i++) {
            if (i % 3 == 0) {
                txs.get(i).commit();
            } else {
                txs.get(i).rollback();
            }
        }

        Transaction tx = CLUSTER.aliveNode().transactions().begin();

        try {
            Object[] expected = makeExpectedRow((InternalTransaction) tx, nodeIdToName);
            List<List<Object>> resultRow = sql(tx, "SELECT COORDINATOR_NODE_ID, TRANSACTION_STATE, TRANSACTION_ID, TRANSACTION_START_TIME, "
                    + " TRANSACTION_TYPE, TRANSACTION_PRIORITY FROM SYSTEM.TRANSACTIONS");

            assertThat(resultRow, hasSize(1));
            assertThat(resultRow.get(0), equalTo(Arrays.asList(expected)));
        } finally {
            tx.rollback();
        }
    }

    @Test
    public void testTransactionLabel() {
        String customLabel = "TEST-CUSTOM-LABEL";
        Transaction tx = CLUSTER.aliveNode().transactions().begin(new TransactionOptions().label(customLabel));
        InternalTransaction internalTx = (InternalTransaction) tx;

        try {
            UUID txId = internalTx.id();

            // Verify the label appears in the system view
            assertQuery("SELECT TRANSACTION_LABEL FROM SYSTEM.TRANSACTIONS WHERE TRANSACTION_ID = '" + txId + "'")
                    .returns(customLabel)
                    .check();
        } finally {
            tx.rollback();
        }
    }

    private static Object[] makeExpectedRow(InternalTransaction tx, Map<UUID, String> nodeIdToName) {
        return new Object[]{
                nodeIdToName.get(tx.coordinatorId()),
                tx.state() == null ? null : tx.state().name(),
                tx.id().toString(),
                // We do not use startTimestamp(), because this method actually has a misname and
                // it returns the so-called "schema synchronization timestamp", and this timestamp
                // may not be exactly the same that is the actual time when the transaction was created.
                Instant.ofEpochMilli(TransactionIds.beginTimestamp(tx.id()).getPhysical()),
                tx.isReadOnly() ? TransactionsViewProvider.READ_ONLY : TransactionsViewProvider.READ_WRITE,
                TransactionIds.priority(tx.id()).name()
        };
    }
}
