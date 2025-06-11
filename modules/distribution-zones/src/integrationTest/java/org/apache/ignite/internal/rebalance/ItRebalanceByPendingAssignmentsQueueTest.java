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

package org.apache.ignite.internal.rebalance;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestRebalanceUtil.pendingPartitionAssignmentsKey;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.partitiondistribution.PendingAssignmentsCalculator.pendingAssignmentsCalculator;
import static org.apache.ignite.internal.rebalance.ItRebalanceByPendingAssignmentsQueueTest.AssignmentsRecorder.recordAssignmentsEvents;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestRebalanceUtil;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.disaster.TestDisasterRecoveryUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for rebalance - handling pending {@link AssignmentsQueue} by {@link RaftGroupEventsListener}.
 */
class ItRebalanceByPendingAssignmentsQueueTest extends ClusterPerTestIntegrationTest {
    private static final int AUTO_SCALE_TIMEOUT_SECONDS = 2;
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected int initialNodes() {
        return 4;
    }

    @Test
    void testDoStableKeySwitchWhenPendingQueueIsOne() {
        createZoneAndTable(4, 2);

        Set<Assignment> stableAssignments = stablePartitionAssignments(TABLE_NAME);

        // update pending assignments by remove some in stable
        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsReduce(stableAssignments);
        assertThat("pending queue size = 1", expectedPendingAssignmentsQueue.size(), equalTo(1));

        recordAssignmentsEvents(cluster.aliveNode(), recorder -> {

            putPendingAssignments(cluster.aliveNode(), TABLE_NAME, expectedPendingAssignmentsQueue);

            await().untilAsserted(() -> {
                Deque<AssignmentsQueue> pendingEvents = recorder.pendingAssignmentsEvents(TABLE_NAME);
                Deque<Assignments> stableEvents = recorder.stableAssignmentsEvents(TABLE_NAME);

                assertThat("pending assignment events N=1", pendingEvents, hasSize(1));
                assertThat(pendingEvents.peekFirst(), equalTo(expectedPendingAssignmentsQueue));

                assertThat("only one stable switch", stableEvents, hasSize(1));
                assertThat(stableEvents.peekLast(), equalTo(pendingEvents.peekLast().peekLast()));
            });
        });
    }

    @Test
    void testDoStableKeySwitchWhenPendingQueueIsGreaterThanOne() {
        createZoneAndTable(4, 2);

        Set<Assignment> stableAssignments = stablePartitionAssignments(TABLE_NAME);

        // update pending assignments by promote/demote some in stable
        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsPromoteDemote(stableAssignments);
        assertThat("pending queue size >= 2", expectedPendingAssignmentsQueue.size(), greaterThanOrEqualTo(2));

        recordAssignmentsEvents(cluster.aliveNode(), recorder -> {

            putPendingAssignments(cluster.aliveNode(), TABLE_NAME, expectedPendingAssignmentsQueue);

            await().untilAsserted(() -> {
                Deque<AssignmentsQueue> pendingEvents = recorder.pendingAssignmentsEvents(TABLE_NAME);
                Deque<Assignments> stableEvents = recorder.stableAssignmentsEvents(TABLE_NAME);

                assertThat("pending assignment events N=queue size", pendingEvents, hasSize(expectedPendingAssignmentsQueue.size()));
                assertThat(pendingEvents.peekFirst(), equalTo(expectedPendingAssignmentsQueue));

                assertThat("only one stable switch", stableEvents, hasSize(1));
                assertThat(stableEvents.peekLast(), equalTo(pendingEvents.peekLast().peekLast()));
            });
        });
    }

    @Test
    void testScaleUp() {
        createZoneAndTable(2, 2);

        var stable0 = PeersAndLearners.fromAssignments(stablePartitionAssignments(TABLE_NAME));
        assertThat(stable0.peers(), hasSize(2));
        assertThat(stable0.learners(), hasSize(0));

        // new peer should be started
        alterZone(3, 2);
        var stable1 = PeersAndLearners.fromAssignments(stablePartitionAssignments(TABLE_NAME));
        assertThat(stable1.peers().size(), equalTo(3));
        assertThat(stable1.learners().size(), equalTo(0));

        // new learner should be started
        alterZone(4, 2);
        var stable2 = PeersAndLearners.fromAssignments(stablePartitionAssignments(TABLE_NAME));
        assertThat(stable2.peers().size(), equalTo(3));
        assertThat(stable2.learners().size(), equalTo(1));
    }

    @Test
    void testScaleDown() {
        createZoneAndTable(4, 2);

        var stable0 = PeersAndLearners.fromAssignments(stablePartitionAssignments(TABLE_NAME));
        assertThat(stable0.peers().size(), equalTo(3));
        assertThat(stable0.learners().size(), equalTo(1));

        String leaseholder = awaitPrimaryReplica(TABLE_NAME).getLeaseholder();

        // peer goes offline
        String stopNodeName = stable0.peers().stream()
                .filter(p -> !p.consistentId().equals(leaseholder))
                .findFirst().map(Peer::consistentId).orElseThrow();
        cluster.stopNode(stopNodeName);
        await().atMost(30, SECONDS).untilAsserted(() -> {
            var stableNames = stablePartitionAssignments(TABLE_NAME).stream()
                    .map(Assignment::consistentId)
                    .collect(Collectors.toSet());
            assertThat(stableNames, not(hasItem(stopNodeName)));
        });

        // learner promoted to peer
        var stable1 = PeersAndLearners.fromAssignments(stablePartitionAssignments(TABLE_NAME));
        assertThat("learner promoted to peer", stable1.peers(), hasItem(stable0.learners().iterator().next()));
        assertThat(stable1.peers().size(), equalTo(3));
        assertThat(stable1.learners().size(), equalTo(0));
    }

    private void createZoneAndTable(int replicas, int quorum) {
        executeSql(format("CREATE ZONE {} (PARTITIONS 1, REPLICAS {}, QUORUM SIZE {}, AUTO SCALE DOWN {}, AUTO SCALE UP {}) STORAGE PROFILES ['default']",
                ZONE_NAME, replicas, quorum, AUTO_SCALE_TIMEOUT_SECONDS, AUTO_SCALE_TIMEOUT_SECONDS));
        executeSql(format("CREATE TABLE {} (id INT PRIMARY KEY, name INT) ZONE {}", TABLE_NAME, ZONE_NAME));
        executeSql(format("INSERT INTO {} VALUES (0, 0)", TABLE_NAME));

        await().untilAsserted(this::assertPartitionAssignments);
    }

    private void alterZone(int replicas, int quorum) {
        executeSql(format("ALTER ZONE {} SET (REPLICAS {}, QUORUM SIZE {})", ZONE_NAME, replicas, quorum));
        await().untilAsserted(this::assertPartitionAssignments);
    }

    private void assertPartitionAssignments() {
        CatalogZoneDescriptor zone = latestCatalog().zone(ZONE_NAME);

        Set<Assignment> stableAssignments = stablePartitionAssignments(TABLE_NAME);

        if (zone.replicas() > cluster.runningNodes().count()) {
            assertThat(stableAssignments, hasSize((int) cluster.runningNodes().count()));
        } else {
            assertThat(stableAssignments, hasSize(zone.replicas()));
        }

        long stablePeerCount = stableAssignments.stream().filter(Assignment::isPeer).count();
        assertThat(stablePeerCount, equalTo((long) zone.consensusGroupSize()));
    }

    private Set<Assignment> stablePartitionAssignments(String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());
        TableViewInternal table = unwrapTableViewInternal(ignite.distributedTableManager().table(tableName));
        var fut = TestRebalanceUtil.stablePartitionAssignments(ignite.metaStorageManager(), table, 0);
        assertThat(fut, willCompleteSuccessfully());
        return ofNullable(fut.join()).orElse(Set.of());
    }

    private static PartitionGroupId partitionId(Ignite ignite, String tableName, int partId) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
        TableViewInternal tableViewInternal = unwrapTableViewInternal(igniteImpl.distributedTableManager().table(tableName));
        // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId, remove.
        return enabledColocation()
                ? new ZonePartitionId(tableViewInternal.zoneId(), partId)
                : new TablePartitionId(tableViewInternal.tableId(), partId);
    }

    private static void putPendingAssignments(Ignite ignite, String tableName, AssignmentsQueue pendingAssignmentsQueue) {
        ByteArray pendingKey = pendingPartitionAssignmentsKey(partitionId(ignite, tableName, 0));
        byte[] pendingVal = pendingAssignmentsQueue.toBytes();
        unwrapIgniteImpl(ignite).metaStorageManager().put(pendingKey, pendingVal).join();
    }

    private ReplicaMeta awaitPrimaryReplica(String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());
        PartitionGroupId partitionId = partitionId(ignite, tableName, 0);
        CompletableFuture<ReplicaMeta> fut = ignite.placementDriver()
                .awaitPrimaryReplica(partitionId, ignite.clock().now(), 30, SECONDS);

        assertThat(fut, willSucceedIn(60, SECONDS));
        return fut.join();
    }

    private Set<String> realAssignments(String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());
        TableViewInternal table = unwrapTableViewInternal(ignite.distributedTableManager().table(tableName));
        return TestDisasterRecoveryUtils.getRealAssignments(
                ignite.disasterRecoveryManager(), ZONE_NAME, table.zoneId(), table.tableId(), 0);
    }

    private AssignmentsQueue assignmentsPromoteDemote(Set<Assignment> stableAssignments) {
        var copy = new HashSet<Assignment>();
        int flips = 2;
        for (Assignment a : stableAssignments) {
            if (flips > 0) {
                copy.add(a.isPeer() ? Assignment.forLearner(a.consistentId()) : Assignment.forPeer(a.consistentId()));
                flips--;
            } else {
                copy.add(a);
            }
        }

        long timestamp = latestCatalog().time();
        return pendingAssignmentsCalculator()
                .stable(Assignments.of(stableAssignments, timestamp))
                .target(Assignments.of(copy, timestamp))
                .toQueue();
    }

    private AssignmentsQueue assignmentsReduce(Set<Assignment> stableAssignments) {
        var copy = new HashSet<>(stableAssignments);
        copy.remove(stableAssignments.iterator().next());

        long timestamp = latestCatalog().time();
        return pendingAssignmentsCalculator()
                .stable(Assignments.of(stableAssignments, timestamp))
                .target(Assignments.of(copy, timestamp))
                .toQueue();
    }

    private Catalog latestCatalog() {
        CatalogManager catalogManager = unwrapIgniteImpl(cluster.node(0)).catalogManager();
        return catalogManager.catalog(catalogManager.latestCatalogVersion());
    }

    static class AssignmentsRecorder implements WatchListener {
        private final Map<String, Deque<AssignmentsQueue>> pendingAssignmentsEvents = new ConcurrentHashMap<>();
        private final Map<String, Deque<Assignments>> stableAssignmentsEvents = new ConcurrentHashMap<>();
        private IgniteImpl ignite;

        static void recordAssignmentsEvents(Ignite ignite, Consumer<AssignmentsRecorder> consumer) {
            var tracker = new AssignmentsRecorder().register(ignite);
            consumer.accept(tracker);
            tracker.unregister();
            tracker.clear();
        }

        private AssignmentsRecorder() {}

        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            Entry entry = event.entryEvent().newEntry();

            if (entry.value() == null) {
                return nullCompletedFuture();
            }

            var partitionId = new String(entry.key());

            if (partitionId.startsWith(RebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX)
                    || partitionId.startsWith(ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX)) {

                pendingAssignmentsEvents.computeIfAbsent(partitionId, k -> new ConcurrentLinkedDeque<>())
                        .add(AssignmentsQueue.fromBytes(entry.value()));
            }

            if (partitionId.startsWith(RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX)
                    || partitionId.startsWith(ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX)) {

                stableAssignmentsEvents.computeIfAbsent(partitionId, k -> new ConcurrentLinkedDeque<>())
                        .add(Assignments.fromBytes(entry.value()));
            }

            return nullCompletedFuture();
        }

        Deque<AssignmentsQueue> pendingAssignmentsEvents(String tableName) {
            return assignments(pendingAssignmentsEvents, tableName);
        }

        Deque<Assignments> stableAssignmentsEvents(String tableName) {
            return assignments(stableAssignmentsEvents, tableName);
        }

        <T> Deque<T> assignments(Map<String, Deque<T>> assignments, String tableName) {
            PartitionGroupId partitionId = partitionId(ignite, tableName, 0);

            for (Map.Entry<String, Deque<T>> entry : assignments.entrySet()) {
                if (entry.getKey().endsWith(partitionId.toString())
                        && !entry.getValue().isEmpty()) {
                    return entry.getValue();
                }
            }
            throw new AssertionError("no assignments events found for " + partitionId);
        }

        void clear() {
            pendingAssignmentsEvents.clear();
            stableAssignmentsEvents.clear();
        }

        private AssignmentsRecorder register(Ignite ignite) {
            this.ignite = unwrapIgniteImpl(ignite);
            MetaStorageManager metaStorageManager = this.ignite.metaStorageManager();

            metaStorageManager.registerPrefixWatch(new ByteArray(ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES), this);
            metaStorageManager.registerPrefixWatch(new ByteArray(ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES), this);

            metaStorageManager.registerPrefixWatch(new ByteArray(RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES), this);
            metaStorageManager.registerPrefixWatch(new ByteArray(RebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES), this);
            return this;
        }

        private void unregister() {
            ignite.metaStorageManager().unregisterWatch(this);
        }
    }
}
