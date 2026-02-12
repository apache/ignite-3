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

import static java.util.stream.Collectors.toUnmodifiableList;
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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
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
import org.apache.ignite.internal.table.distributed.schema.CheckMetadataSufficiencyOnAppendEntries;
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
        inspector = LogInspector.create(CheckMetadataSufficiencyOnAppendEntries.class, true);
    }

    @AfterEach
    void cleanup() {
        if (inspector != null) {
            inspector.stop();
        }
    }

    @Test
    void operationBlockedOnSchemaSyncDoesNotPreventNodeStop() throws Exception {
        InhibitorAndFuture inhibitorAndFuture = producePutHangingDueToSchemaSyncInStateMachines(InhibitionTarget.LEADER);

        assertTimeoutPreemptively(
                Duration.of(10, ChronoUnit.SECONDS),
                () -> cluster.stopNode(0)
        );

        //noinspection ThrowableNotThrown
        assertWillThrowCausedBy(inhibitorAndFuture.future, NodeStoppingException.class);
    }

    private InhibitorAndFuture producePutHangingDueToSchemaSyncInStateMachines(InhibitionTarget inhibitionTarget)
            throws InterruptedException {
        Ignite node = cluster.node(0);

        createTableWith1PartitionOnAllNodes(node);

        cluster.transferLeadershipTo(0, cluster.solePartitionId(ZONE_NAME));

        KeyValueView<Integer, String> kvView = node.tables()
                .table(TABLE_NAME)
                .keyValueView(Integer.class, String.class);

        CompletableFuture<CompositeSchemaInhibitor> inhibitorFuture = startInhibitingSchemaSyncWhenUpdateCommandArrives(inhibitionTarget);

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

    private CompletableFuture<CompositeSchemaInhibitor> startInhibitingSchemaSyncWhenUpdateCommandArrives(
            InhibitionTarget target
    ) {
        AtomicBoolean startedInhibiting = new AtomicBoolean();
        CompletableFuture<CompositeSchemaInhibitor> future = new CompletableFuture<>();

        for (Ignite node : cluster.nodes()) {
            IgniteImpl igniteImpl = unwrapIgniteImpl(node);

            igniteImpl.dropMessages((recipientName, message) -> {
                if (message instanceof WriteActionRequest) {
                    WriteActionRequest actionRequest = (WriteActionRequest) message;

                    if (PartitionGroupId.matchesString(actionRequest.groupId())
                            && actionRequest.deserializedCommand() instanceof UpdateCommand
                            && startedInhibiting.compareAndSet(false, true)) {
                        CompositeSchemaInhibitor schemaSyncInhibitor = CompositeSchemaInhibitor.forNodesNamed(
                                target.nodeNames(cluster),
                                cluster
                        );
                        schemaSyncInhibitor.startInhibit();

                        // Making sure that commitTs (for which we take partition safe time) will be at least DelayDuration ahead
                        // of Metastorage safe time, so during schema sync we'll hang until inhibition is over.
                        if (target == InhibitionTarget.LEADER) {
                            // We got the ActionRequest on igniteImpl, so it's the leader.
                            waitForAllSafeTimesToReach(igniteImpl.clock().current().tick(), igniteImpl);
                        } else {
                            // 1 and 2 are followers.
                            waitForFollowersSafeTimeAdvancesToBeBlocked(cluster);
                        }

                        future.complete(schemaSyncInhibitor);
                    }
                }

                return false;
            });
        }

        return future;
    }

    private CountDownLatch metadataNotAvailableDueToSafeTimeLatch(Cluster cluster, int targetNodeIndex) {
        CountDownLatch latch = new CountDownLatch(1);

        inspector.addHandler(
                event -> {
                    String formattedMessage = event.getMessage().getFormattedMessage();
                    return formattedMessage.contains("Metadata not yet available by safe time")
                            && event.getThreadName().contains(cluster.nodeName(targetNodeIndex));
                },
                latch::countDown
        );

        return latch;
    }

    private static void waitForLatch(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(e);
        }
    }

    private void waitForFollowersSafeTimeAdvancesToBeBlocked(Cluster cluster) {
        CountDownLatch latch1 = metadataNotAvailableDueToSafeTimeLatch(cluster, 1);
        CountDownLatch latch2 = metadataNotAvailableDueToSafeTimeLatch(cluster, 2);

        waitForLatch(latch1);
        waitForLatch(latch2);
    }

    private void waitForAllSafeTimesToReach(HybridTimestamp current, Ignite nodeToWaitSafeTime) {
        ZonePartitionResources zonePartitionResources = unwrapIgniteImpl(nodeToWaitSafeTime)
                .partitionReplicaLifecycleManager()
                .zonePartitionResources(cluster.solePartitionId(ZONE_NAME));

        try {
            zonePartitionResources.safeTimeTracker().waitFor(current).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitTillCommandStartsExecutionAndBlocksOnSchemaSync() throws InterruptedException {
        // Current implementation doesn't actually block any threads, but we still have it a chance to get stuck there is the implementation
        // gets changed.
        Thread.sleep(1000);
    }

    @Test
    void operationBlockedBySchemaHangOnLeaderSucceedsOnSchemaCaughtUp() throws Exception {
        InhibitorAndFuture inhibitorAndFuture = producePutHangingDueToSchemaSyncInStateMachines(InhibitionTarget.LEADER);

        assertThat(inhibitorAndFuture.future, willTimeoutIn(1, TimeUnit.SECONDS));

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

    @Test
    void operationBlockedBySchemaHangOnFollowersSucceedsOnSchemaCaughtUp() throws Exception {
        InhibitorAndFuture inhibitorAndFuture = producePutHangingDueToSchemaSyncInStateMachines(InhibitionTarget.FOLLOWERS);

        waitForFollowersSafeTimeAdvancesToBeBlocked(cluster);

        assertThat(inhibitorAndFuture.future, willTimeoutIn(1, TimeUnit.SECONDS));

        inhibitorAndFuture.inhibitor.stopInhibit();

        assertThat(inhibitorAndFuture.future, willCompleteSuccessfully());

        waitTillStoragesOnAllNodesHaveOneRow();
    }

    private static class CompositeSchemaInhibitor {
        private final List<SchemaSyncInhibitor> inhibitors;

        static CompositeSchemaInhibitor forNodesNamed(Set<String> nodeNames, Cluster cluster) {
            List<SchemaSyncInhibitor> inhibitors = cluster.nodes().stream()
                    .filter(node -> nodeNames.contains(node.name()))
                    .map(SchemaSyncInhibitor::new)
                    .collect(toUnmodifiableList());
            return new CompositeSchemaInhibitor(inhibitors);
        }

        private CompositeSchemaInhibitor(List<SchemaSyncInhibitor> inhibitors) {
            this.inhibitors = inhibitors;
        }

        void startInhibit() {
            for (SchemaSyncInhibitor inhibitor : inhibitors) {
                inhibitor.startInhibit();
            }
        }

        void stopInhibit() {
            for (SchemaSyncInhibitor inhibitor : inhibitors) {
                inhibitor.stopInhibit();
            }
        }
    }

    private static class InhibitorAndFuture {
        private final CompositeSchemaInhibitor inhibitor;
        private final CompletableFuture<Void> future;

        private InhibitorAndFuture(CompositeSchemaInhibitor inhibitor, CompletableFuture<Void> future) {
            this.inhibitor = inhibitor;
            this.future = future;
        }
    }

    private enum InhibitionTarget {
        LEADER {
            @Override
            Set<String> nodeNames(Cluster cluster) {
                return Set.of(cluster.nodeName(0));
            }
        },
        FOLLOWERS {
            @Override
            Set<String> nodeNames(Cluster cluster) {
                return Set.of(cluster.nodeName(1), cluster.nodeName(2));
            }
        };

        abstract Set<String> nodeNames(Cluster cluster);
    }
}
