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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ComponentStoppingException;
import org.apache.ignite.internal.partition.replicator.handlers.WriteIntentSwitchRequestHandler;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.tx.impl.TxCleanupRequestHandler;
import org.junit.jupiter.api.Test;

/** Tests for {@link WriteIntentSwitchRequestHandler}. */
public class ItWriteIntentSwitchRequestHandlerTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(clusterConfig());
    }

    private static String clusterConfig() {
        return "{\n"
                + "  ignite.gc.lowWatermark {\n"
                + "    dataAvailabilityTimeMillis: 1000,\n"
                + "    updateIntervalMillis: 100\n"
                + "  },\n"
                // Default is 60 seconds, and we need to retry write intent resolution in some tests.
                + "  ignite.replication.rpcTimeoutMillis: 1000\n"
                + "}";
    }

    protected static String aggressiveLowWatermarkIncreaseClusterConfig() {
        return "{\n"
                + "  ignite.gc.lowWatermark {\n"
                + "    dataAvailabilityTimeMillis: 1000,\n"
                + "    updateIntervalMillis: 100\n"
                + "  },\n"
                // Default 60 seconds, and we need to retry write intent resolution during test.
                + "  ignite.replication.rpcTimeoutMillis: 1000\n"
                + "}";
    }

    @Test
    void testWriteIntentResolutionAfterTableAlreadyDestroyed() {
        String tableName = "test";

        executeSql("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val INT)");

        // This node will send WriteIntentSwitchReplicaRequest messages.
        IgniteImpl senderNode = unwrapIgniteImpl(cluster.node(0));

        int receiverNodeIndex = 1;
        // This node will process WriteIntentSwitchReplicaRequest messages.
        IgniteImpl receiverNode = unwrapIgniteImpl(cluster.node(receiverNodeIndex));

        CompletableFuture<Void> tableDestroyedFut = new CompletableFuture<>();

        failIntentSwitchUntilTableIsDestroyed(senderNode, tableDestroyedFut);

        receiverNode.transactions().runInTransaction((tx) -> {
            for (int i = 0; i < 10; i++) {
                executeSql(receiverNodeIndex, tx, "INSERT INTO " + tableName + " (id, val) VALUES (?, ?)", i, i);
            }
        });

        executeSql("DROP TABLE " + tableName);

        // Await real table destruction.
        await().until(() -> receiverNode.distributedTableManager().cachedTable(tableName) == null);

        LogInspector logInspector = new LogInspector(
                TxCleanupRequestHandler.class.getName(),
                event -> hasCause(event.getThrown(), ComponentStoppingException.class, "Table is already destroyed")
        );

        logInspector.start();

        try {
            tableDestroyedFut.complete(null);

            // We need to wait for the next write intent switch attempt.
            await().until(logInspector::isMatched);
        } finally {
            logInspector.stop();
        }
    }

    private static void failIntentSwitchUntilTableIsDestroyed(IgniteImpl node0, CompletableFuture<Void> tableDroppedFut) {
        node0.dropMessages((recipientId, message) ->
                message.getClass().getName().contains("WriteIntentSwitchReplicaRequest") && !tableDroppedFut.isDone());
    }
}
