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

import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.partitiondistribution.PendingAssignmentsCalculator.pendingAssignmentsCalculator;
import static org.apache.ignite.internal.rebalance.ItRebalanceByPendingAssignmentsQueueTest.AssignmentsRecorder.recordAssignmentsEvents;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.RunnableX;
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
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.disaster.TestDisasterRecoveryUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for rebalance - handling pending {@link AssignmentsQueue} by {@link RaftGroupEventsListener}.
 */
@Timeout(120)
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

        Set<Assignment> stableAssignments = stablePartitionAssignments();

        // update pending assignments by remove some in stable
        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsReduce(stableAssignments);
        assertThat("pending queue size = 1", expectedPendingAssignmentsQueue.size(), equalTo(1));

        recordAssignmentsEvents(cluster.aliveNode(), recorder -> {

            putPendingAssignments(cluster.aliveNode(), expectedPendingAssignmentsQueue);

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

        Set<Assignment> stableAssignments = stablePartitionAssignments();

        // update pending assignments by promote/demote some in stable
        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsPromoteDemote(stableAssignments);
        assertThat("pending queue size >= 2", expectedPendingAssignmentsQueue.size(), greaterThanOrEqualTo(2));

        recordAssignmentsEvents(cluster.aliveNode(), recorder -> {

            putPendingAssignments(cluster.aliveNode(), expectedPendingAssignmentsQueue);

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

        var stable0 = PeersAndLearners.fromAssignments(stablePartitionAssignments());
        assertThat(stable0.peers(), hasSize(2));
        assertThat(stable0.learners(), hasSize(0));

        // new peer should be started
        alterZone(3, 2);
        var stable1 = PeersAndLearners.fromAssignments(stablePartitionAssignments());
        assertThat(stable1.peers(), hasSize(3));
        assertThat(stable1.learners(), hasSize(0));

        // new learner should be started
        alterZone(4, 2);
        var stable2 = PeersAndLearners.fromAssignments(stablePartitionAssignments());
        assertThat(stable2.peers(), hasSize(3));
        assertThat(stable2.learners(), hasSize(1));
    }

    @Test
    void testScaleDown() {
        createZoneAndTable(4, 2);

        var stable0 = PeersAndLearners.fromAssignments(stablePartitionAssignments());
        assertThat(stable0.peers(), hasSize(3));
        assertThat(stable0.learners(), hasSize(1));

        String leaseholder = primaryReplicaMeta().getLeaseholder();

        // peer goes offline
        String stopNodeName = stable0.peers().stream()
                .filter(p -> !p.consistentId().equals(leaseholder) && isNotMetastoreNode(p))
                .findFirst().map(Peer::consistentId).orElseThrow();
        cluster.stopNode(stopNodeName);
        await().atMost(30, SECONDS).untilAsserted(() -> {
            var stableNames = stablePartitionAssignments().stream()
                    .map(Assignment::consistentId).collect(toSet());
            assertThat(stableNames, not(hasItem(stopNodeName)));
        });

        // learner promoted to peer
        var stable1 = PeersAndLearners.fromAssignments(stablePartitionAssignments());
        assertThat("learner promoted to peer", stable1.peers(), hasItem(stable0.learners().iterator().next()));
        assertThat(stable1.peers(), hasSize(3));
        assertThat(stable1.learners(), hasSize(0));
    }

    @Test
    void testRebalanceWhenPeersMajorityIsLostAndNoAvailableLearners() {
        // need 5 nodes for this test: 3 replicas + 2 empty, stop 2 replicas, 3 replicas after rebalance (1 old replica + 2 old empty)
        cluster.startNode(initialNodes());
        assertThat(cluster.runningNodes().count(), equalTo(5L));

        createZoneAndTable(3, 2);

        var stable0 = PeersAndLearners.fromAssignments(stablePartitionAssignments());
        assertThat(stable0.peers(), hasSize(3));
        assertThat(stable0.learners(), hasSize(0));

        // majority goes offline
        Stream<Peer> quorumPeers = stable0.peers().stream().filter(this::isNotMetastoreNode).limit(2);
        Stream<Peer> learnerPeers = stable0.learners().stream(); // prevent promote learner into peer if any
        List<String> stopNodes = Stream.concat(learnerPeers, quorumPeers).map(Peer::consistentId).collect(toList());
        assertThat(stopNodes, hasSize(2));
        assertThat(realAssignments(), hasItems(stopNodes.toArray(String[]::new)));

        stopNodesInParallel(cluster, stopNodes);

        IgniteImpl aliveNode = findNode(n -> !stopNodes.contains(n.name()));

        await().atMost(60, SECONDS).untilAsserted(() -> {
            assertThat("wait for scale", dataNodes(aliveNode), hasSize(3));
        });

        DisasterRecoveryManager recoveryMgr = aliveNode.disasterRecoveryManager();
        CompletableFuture<Void> resetFut = recoveryMgr.resetPartitions(
                ZONE_NAME,
                emptySet(),
                true,
                -1
        );
        assertThat(resetFut, willSucceedIn(60, SECONDS));

        await().atMost(60, SECONDS).untilAsserted(() -> {
            assertThat(dataNodes(aliveNode), hasSize(3));

            Assignments a = stablePartitionAssignmentsValue();
            assertNotNull(a, "stable assignments should not be null");
            assertFalse(a.fromReset(), "stable should not have fromReset flag");
            assertFalse(a.force(), "stable should not have force flag");

            var stable1 = PeersAndLearners.fromAssignments(a.nodes());
            assertThat(stable1.peers(), hasSize(3));
            assertThat(stable1.learners(), hasSize(0));
            assertNull(pendingAssignmentsValue(), "no ongoing or planned rebalance");
        });
    }

    @Test
    void testNodeRestartDuringQueueProcessing() throws InterruptedException {
        createZoneAndTable(4, 2);

        assertTrue(waitForCondition(() -> stablePartitionAssignments().size() == 4, 10_000));

        Set<Assignment> stableAssignments = stablePartitionAssignments();

        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsPromoteDemote(stableAssignments);
        assertThat("pending queue size >= 2", expectedPendingAssignmentsQueue.size(), greaterThanOrEqualTo(2));

        String leaseholder = primaryReplicaMeta().getLeaseholder();
        String restartNode = stableAssignments.stream()
                .map(Assignment::consistentId).filter(name -> !name.equals(leaseholder)).findFirst().orElseThrow();

        putPendingAssignments(raftLeader().leaderHost, expectedPendingAssignmentsQueue);

        cluster.restartNode(cluster.nodeIndex(restartNode));

        await().atMost(60, SECONDS).untilAsserted(() -> {
            assertThat(dataNodes(), hasSize(4));

            var expected = expectedPendingAssignmentsQueue.peekLast().nodes().stream()
                    .map(Assignment::consistentId).collect(toSet());
            var stableNames = stablePartitionAssignments().stream()
                    .map(Assignment::consistentId).collect(toSet());

            assertThat(stableNames, equalTo(expected));
            assertThat(stableNames, hasItem(restartNode));
        });
    }

    @Test
    void testRaftLeaderChangedDuringAssignmentsQueueProcessing() {
        createZoneAndTable(4, 2);

        Set<Assignment> stableAssignments = stablePartitionAssignments();
        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsPromoteDemote(stableAssignments);
        assertThat("pending queue size >= 2", expectedPendingAssignmentsQueue.size(), greaterThanOrEqualTo(2));

        var leaderBefore = raftLeader();

        // TODO https://issues.apache.org/jira/browse/IGNITE-26023 leader re-election and rebalance are not linearized.
        putPendingAssignments(leaderBefore.leaderHost, expectedPendingAssignmentsQueue);
        // most reliable way to change the leader in raft group is to stop and restart the current one
        cluster.restartNode(cluster.nodeIndex(leaderBefore.leaderHost.name()));

        await().atMost(60, SECONDS).untilAsserted(() -> {
            var leaderAfter = raftLeader();
            assertTrue(leaderAfter.term > leaderBefore.term, "The leader has not changed.");

            var expected = expectedPendingAssignmentsQueue.peekLast().nodes().stream()
                    .map(Assignment::consistentId).collect(toSet());
            var stableNames = stablePartitionAssignments().stream()
                    .map(Assignment::consistentId).collect(toSet());

            assertThat(stableNames, equalTo(expected));
            assertThat(stableNames, hasItem(leaderBefore.leaderHost.name()));
            assertThat(stableNames, hasItem(leaderAfter.leaderHost.name()));
        });
    }

    private void createZoneAndTable(int replicas, int quorum) {
        executeSql(format("CREATE ZONE {} ("
                        + "PARTITIONS 1, REPLICAS {}, QUORUM SIZE {}, AUTO SCALE DOWN {}, AUTO SCALE UP {}"
                        + ") STORAGE PROFILES ['default']",
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

        long runningNodesCount = cluster.runningNodes().count();
        Set<Assignment> stableAssignments = stablePartitionAssignments();

        if (Objects.requireNonNull(zone).replicas() > runningNodesCount) {
            assertThat(stableAssignments, hasSize((int) runningNodesCount));
        } else {
            assertThat(stableAssignments, hasSize(zone.replicas()));
        }

        long stablePeerCount = stableAssignments.stream().filter(Assignment::isPeer).count();
        if (Objects.requireNonNull(zone).consensusGroupSize() > runningNodesCount) {
            assertThat(stablePeerCount, equalTo(runningNodesCount));
        } else {
            assertThat(stablePeerCount, equalTo((long) zone.consensusGroupSize()));
        }
    }

    private Set<Assignment> stablePartitionAssignments() {
        IgniteImpl ignite = raftLeader().leaderHost;
        int zoneId = latestCatalog(ignite).zone(ZONE_NAME).id();

        CompletableFuture<Set<Assignment>> fut = ZoneRebalanceUtil.stablePartitionAssignments(ignite.metaStorageManager(), zoneId, 0);

        assertThat(fut, willCompleteSuccessfully());
        return ofNullable(fut.join()).orElse(Set.of());
    }

    private @Nullable Assignments stablePartitionAssignmentsValue() {
        IgniteImpl ignite = raftLeader().leaderHost;
        CompletableFuture<Entry> fut = ignite.metaStorageManager()
                .get(stablePartAssignmentsKey(partitionGroupId(ignite, 0)));
        assertThat(fut, willCompleteSuccessfully());
        Entry entry = fut.join();
        return entry == null || entry.value() == null ? null : Assignments.fromBytes(entry.value());
    }

    private @Nullable Assignments pendingAssignmentsValue() {
        IgniteImpl ignite = raftLeader().leaderHost;
        CompletableFuture<Entry> fut = ignite.metaStorageManager()
                .get(pendingPartAssignmentsQueueKey(partitionGroupId(ignite, 0)));
        assertThat(fut, willCompleteSuccessfully());
        Entry entry = fut.join();
        return entry == null || entry.value() == null ? null : AssignmentsQueue.fromBytes(entry.value()).poll();
    }

    private static void putPendingAssignments(Ignite ignite, AssignmentsQueue pendingAssignmentsQueue) {
        ByteArray pendingKey = pendingPartAssignmentsQueueKey(partitionGroupId(ignite, 0));
        byte[] pendingVal = pendingAssignmentsQueue.toBytes();

        ByteArray pendingChangeTriggerKey = pendingChangeTriggerKey(partitionGroupId(ignite, 0));
        byte[] pendingChangeTriggerKeyValue = unwrapIgniteImpl(ignite).clock().now().toBytes();
        var keyValsToUpdate = Map.of(pendingKey, pendingVal, pendingChangeTriggerKey, pendingChangeTriggerKeyValue);

        assertThat(unwrapIgniteImpl(ignite).metaStorageManager().putAll(keyValsToUpdate), willCompleteSuccessfully());
    }

    private Set<String> dataNodes() {
        IgniteImpl ignite = raftLeader().leaderHost;
        return dataNodes(ignite);
    }

    private static Set<String> dataNodes(IgniteImpl ignite) {
        long causalityToken = ignite.metaStorageManager().appliedRevision();
        HybridTimestamp timestamp = ignite.metaStorageManager().timestampByRevisionLocally(causalityToken);
        int catalogVersion = ignite.catalogManager().latestCatalogVersion();
        int zoneId = latestCatalog(ignite).zone(ZONE_NAME).id();
        CompletableFuture<Set<String>> dataNodes = ignite.distributionZoneManager().dataNodes(timestamp, catalogVersion, zoneId);
        try {
            return dataNodes.get(20, SECONDS);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static ZonePartitionId partitionGroupId(Ignite ignite, int partId) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
        Catalog catalog = latestCatalog(igniteImpl);

        return  new ZonePartitionId(catalog.zone(ZONE_NAME).id(), partId);
    }

    private ReplicaMeta primaryReplicaMeta() {
        return primaryReplicaMeta(raftLeader().leaderHost);
    }

    private ReplicaMeta primaryReplicaMeta(IgniteImpl ignite) {
        ZonePartitionId partitionId = partitionGroupId(ignite, 0);
        CompletableFuture<ReplicaMeta> fut = ignite.placementDriver()
                .awaitPrimaryReplica(partitionId, ignite.clock().now(), 60, SECONDS);

        assertThat(fut, willCompleteSuccessfully());
        return fut.join();
    }

    private LeaderWithTerm raftLeader() {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());
        var raftNodeId = new RaftNodeId(partitionGroupId(ignite, 0), new Peer(ignite.name()));
        var jraftServer = (JraftServerImpl) ignite.raftManager().server();
        RaftGroupService raftGroupService = jraftServer.raftGroupService(raftNodeId);
        String consistentId = ofNullable(raftGroupService)
                .map(RaftGroupService::getRaftNode)
                .map(Node::getLeaderId)
                .map(PeerId::getConsistentId)
                .orElse(null);

        assertNotNull(consistentId);

        return new LeaderWithTerm(
                unwrapIgniteImpl(cluster.node(cluster.nodeIndex(consistentId))),
                raftGroupService.getRaftNode().getCurrentTerm()
        );
    }

    private Set<String> realAssignments() {
        IgniteImpl ignite = raftLeader().leaderHost;
        Catalog catalog = latestCatalog(ignite);
        int zoneId = catalog.zone(ZONE_NAME).id();
        return TestDisasterRecoveryUtils.getRealAssignments(
                ignite.disasterRecoveryManager(), ZONE_NAME, zoneId, 0);
    }

    private boolean isNotMetastoreNode(Peer p) {
        return Arrays.stream(cmgMetastoreNodes()).noneMatch(i -> p.consistentId().equals(cluster.node(i).name()));
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
        return latestCatalog(cluster.node(0));
    }

    private static Catalog latestCatalog(Ignite ignite) {
        CatalogManager catalogManager = unwrapIgniteImpl(ignite).catalogManager();
        return catalogManager.catalog(catalogManager.latestCatalogVersion());
    }

    /**
     * It's important to stop nodes in parallel, not only to save time, but to remove them from data nodes at the same time with a single
     * scale-down event. Otherwise tests will start failing randomly.
     */
    private static void stopNodesInParallel(Cluster cluster, List<String> names) {
        runRace(names.stream().<RunnableX>map(name -> () -> cluster.stopNode(name)).toArray(RunnableX[]::new));
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

            if (partitionId.startsWith(PENDING_ASSIGNMENTS_QUEUE_PREFIX)) {

                pendingAssignmentsEvents.computeIfAbsent(partitionId, k -> new ConcurrentLinkedDeque<>())
                        .add(AssignmentsQueue.fromBytes(entry.value()));
            }

            if (partitionId.startsWith(STABLE_ASSIGNMENTS_PREFIX)) {

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
            ZonePartitionId partitionId = partitionGroupId(ignite, 0);

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

            metaStorageManager.registerPrefixWatch(new ByteArray(PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES), this);
            metaStorageManager.registerPrefixWatch(new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES), this);
            return this;
        }

        private void unregister() {
            ignite.metaStorageManager().unregisterWatch(this);
        }
    }

    private static final class LeaderWithTerm {
        private final IgniteImpl leaderHost;

        private final long term;

        private LeaderWithTerm(IgniteImpl leaderHost, long term) {
            this.leaderHost = leaderHost;
            this.term = term;
        }
    }
}
