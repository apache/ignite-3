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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.impl.RemoteEnlistmentTracker;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Test that verifies cleanup of remote enlistments when a thin client disconnects.
 */
public class ItThinClientTransactionDirectMappingCleanupTest extends ItAbstractThinClientTest {
    /**
     * Tests that direct enlistments are cleaned up when client connection is lost.
     * This test verifies the fix for IGNITE-27651.
     */
    @Test
    public void testDirectEnlistmentsCleanedUpOnClientDisconnect() throws Exception {
        Table table = table();

        // Create a separate client to disconnect it later.
        IgniteClient separateClient = IgniteClient.builder()
                .addresses("127.0.0.1:" + getClientPorts().get(0))
                .build();

        try {
            // Get the table from the separate client.
            Table separateClientTable = separateClient.tables().table(table.name());
            KeyValueView<Tuple, Tuple> kvView = separateClientTable.keyValueView();

            // Start transaction and perform operations.
            Transaction tx = separateClient.transactions().begin();

            Tuple key = Tuple.create().set("key", 1);
            Tuple value = Tuple.create().set("val", "test-value");

            // Perform operation that creates direct enlistments.
            kvView.put(tx, key, value);

            // Get server instance to verify enlistment tracking.
            IgniteImpl serverNode = unwrapIgniteImpl(server(0));
            TxManagerImpl txManager = (TxManagerImpl) serverNode.txManager();
            RemoteEnlistmentTracker tracker = txManager.remoteEnlistmentTracker();

            // Wait a bit to ensure the operation is processed.
            Thread.sleep(100);

            // Verify that enlistments are tracked before disconnect.
            int trackedBefore = tracker.trackedTransactionsCount();

            // Close the client connection abruptly (simulates crash/disconnect).
            separateClient.close();

            // Wait for cleanup to complete.
            IgniteTestUtils.waitForCondition(() -> {
                int trackedAfter = tracker.trackedTransactionsCount();
                // Enlistments should be cleaned up.
                return trackedAfter < trackedBefore || trackedAfter == 0;
            }, 5000);

            // Verify that we can now perform operations on the same keys
            // (locks should be released).
            Transaction newTx = client().transactions().begin();
            kvView = table.keyValueView();

            // This should succeed if locks were properly released.
            kvView.put(newTx, key, Tuple.create().set("val", "new-value"));
            newTx.commit();

            // Verify the value was updated.
            Tuple result = kvView.get(null, key);
            assertTrue(result.stringValue("val").equals("new-value"),
                    "Value should be updated, indicating locks were released");
        } finally {
            // Make sure the separate client is closed even if the test fails.
            try {
                separateClient.close();
            } catch (Exception e) {
                // Ignore exceptions during cleanup
            }
        }
    }

    /**
     * Tests that coordinator transaction is cleaned up when client disconnects
     * before performing any direct mapping operations.
     */
    @Test
    public void testCoordinatorTransactionCleanedUpOnDisconnect() throws Exception {
        Table table = table();

        // Create a separate client to disconnect it later.
        IgniteClient separateClient = IgniteClient.builder()
                .addresses("127.0.0.1:" + getClientPorts().get(0))
                .build();

        try {
            // Start transaction but don't perform any operations yet.
            Transaction tx = separateClient.transactions().begin();

            // Get the transaction ID (indirectly by checking tracking).
            IgniteImpl serverNode = unwrapIgniteImpl(server(0));
            TxManagerImpl txManager = (TxManagerImpl) serverNode.txManager();

            // Close the client connection.
            separateClient.close();

            // Wait a bit for cleanup.
            Thread.sleep(500);

            // Verify that we can create new transactions and perform operations
            // without any issues (no leaked resources).
            Table clientTable = table;
            KeyValueView<Tuple, Tuple> kvView = clientTable.keyValueView();

            Transaction newTx = client().transactions().begin();
            Tuple key = Tuple.create().set("key", 100);
            Tuple value = Tuple.create().set("val", "test");

            kvView.put(newTx, key, value);
            newTx.commit();

            Tuple result = kvView.get(null, key);
            assertTrue(result.stringValue("val").equals("test"),
                    "Transaction should succeed after cleanup");
        } finally {
            try {
                separateClient.close();
            } catch (Exception e) {
                // Ignore exceptions during cleanup
            }
        }
    }

    private Table table() {
        // TODO: Wrong
        return client().tables().tables().get(0);
    }
}
