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
import static org.apache.ignite.internal.TestRebalanceUtil.pendingPartitionAssignmentsKey;
import static org.apache.ignite.internal.TestRebalanceUtil.stablePartitionAssignments;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.partitiondistribution.PendingAssignmentsCalculator.pendingAssignmentsCalculator;
import static org.apache.ignite.internal.rebalance.ItRebalanceByPendingAssignmentsQueueTest.AssignmentsRecorder.recordAssignmentsEvents;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
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
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableViewInternal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for rebalance - handling pending {@link AssignmentsQueue} by {@link RaftGroupEventsListener}.
 */
class ItRebalanceByPendingAssignmentsQueueTest extends ClusterPerTestIntegrationTest {

    @Override
    protected int initialNodes() {
        return 4;
    }

    @BeforeEach
    void setUp() {
        executeSql("CREATE ZONE TEST_ZONE WITH PARTITIONS=1, REPLICAS=4, QUORUM_SIZE=2, STORAGE_PROFILES='default'");
        executeSql("CREATE TABLE TEST_TABLE (id INT PRIMARY KEY, name INT) ZONE TEST_ZONE");
        executeSql("INSERT INTO TEST_TABLE VALUES (0, 0)");

        // await partition assignments
        await().untilAsserted(() -> {
            CatalogZoneDescriptor zone = latestCatalog().zone("TEST_ZONE");

            Set<Assignment> stableAssignments = getPartitionStableAssignments(cluster.aliveNode(), "TEST_TABLE");
            assertThat(stableAssignments, hasSize(zone.replicas()));

            long stablePeerCount = stableAssignments.stream().filter(Assignment::isPeer).count();
            assertThat(stablePeerCount, equalTo((long) zone.consensusGroupSize()));
        });
    }

    @Test
    void testDoStableKeySwitchWhenPendingQueueIsOne() {
        Set<Assignment> stableAssignments = getPartitionStableAssignments(cluster.aliveNode(), "TEST_TABLE");

        // update pending assignments by remove some in stable
        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsReduce(stableAssignments);
        assertThat("pending queue size = 1", expectedPendingAssignmentsQueue.size(), equalTo(1));

        recordAssignmentsEvents(cluster.aliveNode(), recorder -> {

            putPendingAssignments(cluster.aliveNode(), "TEST_TABLE", expectedPendingAssignmentsQueue);

            await().untilAsserted(() -> {
                Deque<AssignmentsQueue> pendingEvents = recorder.pendingAssignmentsEvents("TEST_TABLE");
                Deque<Assignments> stableEvents = recorder.stableAssignmentsEvents("TEST_TABLE");

                assertThat("pending assignment events N=1", pendingEvents, hasSize(1));
                assertThat(pendingEvents.peekFirst(), equalTo(expectedPendingAssignmentsQueue));

                assertThat("only one stable switch", stableEvents, hasSize(1));
                assertThat(stableEvents.peekLast(), equalTo(pendingEvents.peekLast().peekLast()));
            });
        });
    }

    @Test
    void testDoStableKeySwitchWhenPendingQueueIsGreaterThanOne() {
        Set<Assignment> stableAssignments = getPartitionStableAssignments(cluster.aliveNode(), "TEST_TABLE");

        // update pending assignments by promote/demote some in stable
        AssignmentsQueue expectedPendingAssignmentsQueue = assignmentsPromoteDemote(stableAssignments);
        assertThat("pending queue size >= 2", expectedPendingAssignmentsQueue.size(), greaterThanOrEqualTo(2));

        recordAssignmentsEvents(cluster.aliveNode(), recorder -> {

            putPendingAssignments(cluster.aliveNode(), "TEST_TABLE", expectedPendingAssignmentsQueue);

            await().untilAsserted(() -> {
                Deque<AssignmentsQueue> pendingEvents = recorder.pendingAssignmentsEvents("TEST_TABLE");
                Deque<Assignments> stableEvents = recorder.stableAssignmentsEvents("TEST_TABLE");

                assertThat("pending assignment events N=queue size", pendingEvents, hasSize(expectedPendingAssignmentsQueue.size()));
                assertThat(pendingEvents.peekFirst(), equalTo(expectedPendingAssignmentsQueue));

                assertThat("only one stable switch", stableEvents, hasSize(1));
                assertThat(stableEvents.peekLast(), equalTo(pendingEvents.peekLast().peekLast()));
            });
        });
    }

    private static PartitionGroupId partitionId(Ignite ignite, String tableName) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
        TableViewInternal tableViewInternal = unwrapTableViewInternal(igniteImpl.distributedTableManager().table(tableName));
        // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId, remove.
        return enabledColocation()
                ? new ZonePartitionId(tableViewInternal.zoneId(), 0)
                : new TablePartitionId(tableViewInternal.tableId(), 0);
    }

    private static Set<Assignment> getPartitionStableAssignments(Ignite node, String tableName) {
        IgniteImpl ignite = unwrapIgniteImpl(node);
        TableViewInternal tableViewInternal = unwrapTableViewInternal(ignite.distributedTableManager().table(tableName));
        var fut = stablePartitionAssignments(ignite.metaStorageManager(), tableViewInternal, 0);
        assertThat(fut, willCompleteSuccessfully());
        return ofNullable(fut.join()).orElse(Set.of());
    }

    private static void putPendingAssignments(Ignite ignite, String tableName, AssignmentsQueue pendingAssignmentsQueue) {
        ByteArray pendingKey = pendingPartitionAssignmentsKey(partitionId(ignite, tableName));
        byte[] pendingVal = pendingAssignmentsQueue.toBytes();
        unwrapIgniteImpl(ignite).metaStorageManager().put(pendingKey, pendingVal).join();
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
            PartitionGroupId partitionId = partitionId(ignite, tableName);

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
