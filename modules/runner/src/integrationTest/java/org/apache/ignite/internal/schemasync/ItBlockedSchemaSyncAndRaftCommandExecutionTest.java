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

package org.apache.ignite.internal.schemasync;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowCausedBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.schema.SchemaSyncInhibitor;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class ItBlockedSchemaSyncAndRaftCommandExecutionTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(aggressiveLowWatermarkIncreaseClusterConfig());
    }

    @Test
    void raftThreadBlockedOnSchemaSyncDoesNotPreventNodeStop() throws Exception {
        CompletableFuture<Void> putFuture = producePutBlockedInRaftStateMachine();

        assertTimeoutPreemptively(
                Duration.of(10, SECONDS),
                () -> cluster.stopNode(0)
        );

        assertWillThrowCausedBy(putFuture, NodeStoppingException.class);
    }

    private CompletableFuture<Void> producePutBlockedInRaftStateMachine() throws InterruptedException {
        Ignite node = cluster.node(0);

        node.sql().executeScript("CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR)");

        KeyValueView<Integer, String> kvView = node.tables()
                .table(TABLE_NAME)
                .keyValueView(Integer.class, String.class);

        startInhibitingSchemaSyncWhenUpdateCommandArrives(node);

        CompletableFuture<Void> putFuture = kvView.putAsync(null, 1, "one");

        waitTillCommandStartsExecutionAndBlocksOnSchemaSync();
        return putFuture;
    }

    private static void startInhibitingSchemaSyncWhenUpdateCommandArrives(Ignite node) {
        AtomicBoolean startedInhibiting = new AtomicBoolean();

        unwrapIgniteImpl(node).dropMessages((recipientName, message) -> {
            if (message instanceof WriteActionRequest) {
                WriteActionRequest actionRequest = (WriteActionRequest) message;
                if (PartitionGroupId.matchesString(actionRequest.groupId())
                        && actionRequest.deserializedCommand() instanceof UpdateCommand
                        && startedInhibiting.compareAndSet(false, true)) {
                    new SchemaSyncInhibitor(node).startInhibit();
                }
            }

            return false;
        });
    }

    private static void waitTillCommandStartsExecutionAndBlocksOnSchemaSync() throws InterruptedException {
        Thread.sleep(1000);
    }
}
