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

package org.apache.ignite.internal.placementdriver;

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.partitiondistribution.Assignment.forPeer;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AssignmentsTracker}.
 */
public class AssignmentsTrackerTest extends BaseIgniteAbstractTest {
    private final ReplicationGroupId groupId0 = colocationEnabled() ? new ZonePartitionId(0, 0) : new TablePartitionId(0, 0);
    private final ReplicationGroupId groupId1 = colocationEnabled() ? new ZonePartitionId(1, 0) : new TablePartitionId(1, 0);

    private Set<String> dataNodes0 = emptySet();
    private Set<String> dataNodes1 = emptySet();

    private MetaStorageManager metaStorageManager;

    private AssignmentsTracker assignmentsTracker;

    @BeforeEach
    public void setUp() {
        metaStorageManager = StandaloneMetaStorageManager.create();

        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        assignmentsTracker = new AssignmentsTracker(
                metaStorageManager,
                new TestFailureProcessor(),
                new SystemPropertiesNodeProperties(),
                zoneId -> completedFuture(dataNodes(zoneId)),
                id -> id
        );

        assignmentsTracker.startTrack();
    }

    @AfterEach
    public void tearDown() throws Exception {
        assignmentsTracker.stopTrack();
        assertThat(metaStorageManager.stopAsync(), willCompleteSuccessfully());
    }

    private Set<String> dataNodes(int zoneId) {
        return zoneId == 0 ? dataNodes0 : dataNodes1;
    }

    private ByteArray assignmentsKey(ReplicationGroupId groupId) {
        return colocationEnabled()
                ? ZoneRebalanceUtil.stablePartAssignmentsKey((ZonePartitionId) groupId)
                : RebalanceUtil.stablePartAssignmentsKey((TablePartitionId) groupId);
    }

    @Test
    public void testInitialEmptyAssignmentsWithSuccessfulWaiting() {
        CompletableFuture<List<TokenizedAssignments>> assignmentsListFuture = assignmentsTracker
                .awaitNonEmptyAssignments(List.of(groupId0), metaStorageManager.clusterTime().currentSafeTime(), 10_000);

        assertFalse(assignmentsListFuture.isDone());

        metaStorageManager.put(assignmentsKey(groupId0), Assignments.toBytes(Set.of(forPeer("A")), HybridTimestamp.MIN_VALUE.longValue()));

        assertThat(assignmentsListFuture, willCompleteSuccessfully());

        assertEquals(1, assignmentsListFuture.join().get(0).nodes().size());
    }

    @Test
    public void testChangeAssignmentsForOneGroupWhileWaitingForAnother() {
        CompletableFuture<List<TokenizedAssignments>> assignmentsListFuture = assignmentsTracker
                .awaitNonEmptyAssignments(List.of(groupId0, groupId1), metaStorageManager.clusterTime().currentSafeTime(), 10_000);

        assertFalse(assignmentsListFuture.isDone());

        Set<Assignment> assignmentsForGroup0Initial = Set.of(forPeer("A"), forPeer("B"));
        Set<Assignment> assignmentsForGroup0Updated = Set.of(forPeer("B"), forPeer("C"));
        Set<Assignment> assignmentsForGroup1 = Set.of(forPeer("D"), forPeer("E"));

        dataNodes0 = assignmentsForGroup0Initial.stream().map(Assignment::consistentId).collect(toSet());

        metaStorageManager.put(
                assignmentsKey(groupId0),
                Assignments.toBytes(assignmentsForGroup0Updated, HybridTimestamp.MIN_VALUE.longValue())
        );

        metaStorageManager.put(
                assignmentsKey(groupId1),
                Assignments.toBytes(assignmentsForGroup1, HybridTimestamp.MIN_VALUE.longValue())
        );

        assertThat(assignmentsListFuture, willCompleteSuccessfully());

        assertEquals(assignmentsForGroup0Updated, assignmentsListFuture.join().get(0).nodes());
        assertEquals(assignmentsForGroup1, assignmentsListFuture.join().get(1).nodes());
    }

    private static class TestFailureProcessor implements FailureProcessor {
        @Override
        public boolean process(FailureContext failureCtx) {
            return false;
        }
    }
}
