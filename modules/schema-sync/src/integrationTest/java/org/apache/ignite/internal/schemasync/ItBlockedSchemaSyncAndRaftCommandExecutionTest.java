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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowCausedBy;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.partition.replicator.ZoneResourcesManager.ZonePartitionResources;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.schema.SchemaSyncInhibitor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.schema.CheckCatalogVersionOnAppendEntries;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class ItBlockedSchemaSyncAndRaftCommandExecutionTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String TABLE_NAME = "TEST";

    private LogInspector inspector;

    @Override
    protected int initialNodes() {
        return 3;
    }

    @BeforeEach
    void prepare() {
        inspector = LogInspector.create(CheckCatalogVersionOnAppendEntries.class, true);
    }

    @AfterEach
    void cleanup() {
        inspector.stop();
    }

    @Test
    void operationBlockedOnSchemaSyncDoesNotPreventNodeStop() throws Exception {
        InhibitorAndFuture inhibitorAndFuture = producePutHangingDueToSchemaSyncInLeaderStateMachine();

        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> cluster.stopNode(0));

        //noinspection ThrowableNotThrown
        assertWillThrowCausedBy(inhibitorAndFuture.future, NodeStoppingException.class);
    }

    private InhibitorAndFuture producePutHangingDueToSchemaSyncInLeaderStateMachine()
            throws InterruptedException {
        Ignite node = cluster.node(0);

        createTableWith1PartitionOnAllNodes(node);

        cluster.transferLeadershipTo(0, cluster.solePartitionId(ZONE_NAME));

        KeyValueView<Integer, String> kvView = node.tables()
                .table(TABLE_NAME)
                .keyValueView(Integer.class, String.class);

        CompletableFuture<SchemaSyncInhibitor> inhibitorFuture = startInhibitingSchemaSyncWhenUpdateCommandArrives();

        CompletableFuture<Void> putFuture = kvView.putAsync(null, 1, "one");

        waitTillCommandStartsExecutionAndBlocksOnSchemaSync();

        assertThat(inhibitorFuture, willCompleteSuccessfully());

        return new InhibitorAndFuture(inhibitorFuture.join(), putFuture);
    }

    private static void createTableWith1PartitionOnAllNodes(Ignite node) {
        node.sql().executeScript(
                "CREATE ZONE " + ZONE_NAME + " (REPLICAS 3, PARTITIONS 1) STORAGE PROFILES ['"
                        + DEFAULT_AIPERSIST_PROFILE_NAME + "'];"
                + "CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR) ZONE " + ZONE_NAME + ";"
        );
    }

    private CompletableFuture<SchemaSyncInhibitor> startInhibitingSchemaSyncWhenUpdateCommandArrives() {
        AtomicBoolean startedInhibiting = new AtomicBoolean();
        CompletableFuture<SchemaSyncInhibitor> future = new CompletableFuture<>();

        for (Ignite node : cluster.nodes()) {
            IgniteImpl igniteImpl = unwrapIgniteImpl(node);

            igniteImpl.dropMessages((recipientName, message) -> {
                if (message instanceof WriteActionRequest) {
                    WriteActionRequest actionRequest = (WriteActionRequest) message;

                    if (PartitionGroupId.matchesString(actionRequest.groupId())
                            && actionRequest.deserializedCommand() instanceof UpdateCommand
                            && startedInhibiting.compareAndSet(false, true)) {
                        SchemaSyncInhibitor inhibitor = new SchemaSyncInhibitor(igniteImpl);
                        inhibitor.startInhibit();

                        // Making sure that commitTs (for which we take partition safe time) will be at least DelayDuration ahead
                        // of Metastorage safe time, so during schema sync we'll hang until inhibition is over.
                        waitForAllSafeTimesToReach(igniteImpl.clock().current().tick(), igniteImpl);

                        future.complete(inhibitor);
                    }
                }

                return false;
            });
        }

        return future;
    }

    private void waitForAllSafeTimesToReach(HybridTimestamp current, Ignite nodeToWaitSafeTime) {
        ZonePartitionResources zonePartitionResources = unwrapIgniteImpl(nodeToWaitSafeTime)
                .partitionReplicaLifecycleManager()
                .zonePartitionResources(cluster.solePartitionId(ZONE_NAME));

        try {
            zonePartitionResources.safeTimeTracker().waitFor(current).get(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitTillCommandStartsExecutionAndBlocksOnSchemaSync() throws InterruptedException {
        // Current implementation doesn't actually block any threads, but we still give it a chance to get stuck if the implementation
        // gets changed.
        Thread.sleep(1000);
    }

    @Test
    void operationBlockedBySchemaHangOnLeaderSucceedsOnSchemaCaughtUp() throws Exception {
        InhibitorAndFuture inhibitorAndFuture = producePutHangingDueToSchemaSyncInLeaderStateMachine();

        assertThat(inhibitorAndFuture.future, willTimeoutIn(1, SECONDS));

        inhibitorAndFuture.inhibitor.stopInhibit();

        assertThat(inhibitorAndFuture.future, willCompleteSuccessfully());

        waitTillStoragesOnAllNodesHaveOneRow();
    }

    private void waitTillStoragesOnAllNodesHaveOneRow() {
        for (Ignite node : cluster.nodes()) {
            waitTillStorageHasOneNode(node);
        }
    }

    private static void waitTillStorageHasOneNode(Ignite node) {
        TableImpl table = unwrapTableImpl(node.tables().table(TABLE_NAME));
        MvPartitionStorage partitionStorage = table.internalTable().storage().getMvPartition(0);
        assertThat(partitionStorage, is(notNullValue()));

        bypassingThreadAssertions(() -> await().until(partitionStorage::estimatedSize, is(1L)));
    }

    private static class InhibitorAndFuture {
        private final SchemaSyncInhibitor inhibitor;
        private final CompletableFuture<Void> future;

        private InhibitorAndFuture(SchemaSyncInhibitor inhibitor, CompletableFuture<Void> future) {
            this.inhibitor = inhibitor;
            this.future = future;
        }
    }
}
