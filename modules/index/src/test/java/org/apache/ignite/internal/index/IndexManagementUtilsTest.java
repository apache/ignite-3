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

package org.apache.ignite.internal.index;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.index.IndexManagementUtils.enterBusy;
import static org.apache.ignite.internal.index.IndexManagementUtils.extractIndexIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.extractPartitionIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.inProgressBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.isLocalNode;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.index.IndexManagementUtils.leaveBusy;
import static org.apache.ignite.internal.index.IndexManagementUtils.localNode;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKeyPrefix;
import static org.apache.ignite.internal.index.IndexManagementUtils.toPartitionBuildIndexMetastoreKeyString;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_ID;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.newPrimaryReplicaMeta;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/** For {@link IndexManagementUtils} testing. */
public class IndexManagementUtilsTest extends BaseIgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    @Test
    void testPartitionBuildIndexMetastoreKey() {
        assertEquals(ByteArray.fromString("indexBuild.partition.1.2"), partitionBuildIndexMetastoreKey(1, 2));
        assertEquals(ByteArray.fromString("indexBuild.partition.7.9"), partitionBuildIndexMetastoreKey(7, 9));
    }

    @Test
    void testInProgressBuildIndexMetastoreKey() {
        assertEquals(ByteArray.fromString("indexBuild.inProgress.1"), inProgressBuildIndexMetastoreKey(1));
        assertEquals(ByteArray.fromString("indexBuild.inProgress.7"), inProgressBuildIndexMetastoreKey(7));
    }

    @Test
    void testPartitionBuildIndexMetastoreKeyPrefix() {
        assertEquals(ByteArray.fromString("indexBuild.partition.1"), partitionBuildIndexMetastoreKeyPrefix(1));
        assertEquals(ByteArray.fromString("indexBuild.partition.7"), partitionBuildIndexMetastoreKeyPrefix(7));
    }

    @Test
    void tesToPartitionBuildIndexMetastoreKeyString() {
        assertEquals("indexBuild.partition.1.2", toPartitionBuildIndexMetastoreKeyString("indexBuild.partition.1.2".getBytes(UTF_8)));
        assertEquals("indexBuild.partition.7.9", toPartitionBuildIndexMetastoreKeyString("indexBuild.partition.7.9".getBytes(UTF_8)));
    }

    @Test
    void testExtractPartitionIdFromPartitionBuildIndexKey() {
        assertEquals(2, extractPartitionIdFromPartitionBuildIndexKey("indexBuild.partition.1.2"));
        assertEquals(9, extractPartitionIdFromPartitionBuildIndexKey("indexBuild.partition.7.9"));
    }

    @Test
    void testExtractIndexIdFromPartitionBuildIndexKey() {
        assertEquals(1, extractIndexIdFromPartitionBuildIndexKey("indexBuild.partition.1.2"));
        assertEquals(7, extractIndexIdFromPartitionBuildIndexKey("indexBuild.partition.7.9"));
    }

    @Test
    void testIsPrimaryReplicaTrue() {
        TablePartitionId replicaGroupId = new TablePartitionId(1, 0);

        HybridTimestamp startTime = clock.now();
        long dayInMillis = TimeUnit.DAYS.toMillis(1);

        ReplicaMeta replicaMeta = newPrimaryReplicaMeta(LOCAL_NODE, replicaGroupId, startTime, startTime.addPhysicalTime(dayInMillis));

        assertTrue(isPrimaryReplica(replicaMeta, LOCAL_NODE, clock.now()));
    }

    @Test
    void testIsPrimaryReplicaFalse() {
        TablePartitionId replicaGroupId = new TablePartitionId(1, 0);

        ClusterNode otherNode = new ClusterNodeImpl(NODE_ID + "-other", NODE_NAME + "-other", mock(NetworkAddress.class));

        HybridTimestamp now = clock.now();
        long dayInMillis = TimeUnit.DAYS.toMillis(1);
        long hourInMillis = TimeUnit.HOURS.toMillis(1);

        HybridTimestamp startTime0 = now;
        HybridTimestamp startTime1 = now.addPhysicalTime(-dayInMillis);

        ReplicaMeta replicaMeta0 = newPrimaryReplicaMeta(otherNode, replicaGroupId, startTime0, startTime0.addPhysicalTime(dayInMillis));
        ReplicaMeta replicaMeta1 = newPrimaryReplicaMeta(LOCAL_NODE, replicaGroupId, startTime1, startTime1.addPhysicalTime(hourInMillis));
        ReplicaMeta replicaMeta2 = newPrimaryReplicaMeta(LOCAL_NODE, replicaGroupId, now, now);

        assertFalse(isPrimaryReplica(replicaMeta0, LOCAL_NODE, clock.now()));
        assertFalse(isPrimaryReplica(replicaMeta1, LOCAL_NODE, clock.now()));
        assertFalse(isPrimaryReplica(replicaMeta2, LOCAL_NODE, now));
    }

    @Test
    void testLocalNode() {
        ClusterNode localNode = mock(ClusterNode.class);
        ClusterService clusterService = clusterService(localNode);

        assertEquals(localNode, localNode(clusterService));
    }

    @Test
    void testIsLocalNode() {
        ClusterNode localNode = new ClusterNodeImpl("local-id", "local", new NetworkAddress("127.0.0.1", 8888));
        ClusterNode notLocalNode = new ClusterNodeImpl("not-local-id", "not-local", new NetworkAddress("127.0.0.1", 7777));

        ClusterService clusterService = clusterService(localNode);

        assertTrue(isLocalNode(clusterService, localNode.id()));
        assertFalse(isLocalNode(clusterService, notLocalNode.id()));
    }

    @Test
    void testEnterBusy() {
        IgniteSpinBusyLock busyLock0 = spy(new IgniteSpinBusyLock());
        IgniteSpinBusyLock busyLock1 = spy(new IgniteSpinBusyLock());

        busyLock0.block();
        assertFalse(enterBusy(busyLock0, busyLock1));

        InOrder inOrder = inOrder(busyLock0, busyLock1);

        inOrder.verify(busyLock0).enterBusy();
        inOrder.verify(busyLock1, never()).enterBusy();
        inOrder.verify(busyLock1, never()).leaveBusy();
        inOrder.verify(busyLock1, never()).leaveBusy();

        clearInvocations(busyLock0, busyLock1);

        busyLock0.unblock();
        busyLock1.block();
        assertFalse(enterBusy(busyLock0, busyLock1));

        inOrder.verify(busyLock0).enterBusy();
        inOrder.verify(busyLock1).enterBusy();
        inOrder.verify(busyLock0).leaveBusy();
        inOrder.verify(busyLock1, never()).leaveBusy();

        clearInvocations(busyLock0, busyLock1);

        busyLock1.unblock();
        assertTrue(enterBusy(busyLock0, busyLock1));

        inOrder.verify(busyLock0).enterBusy();
        inOrder.verify(busyLock1).enterBusy();
        inOrder.verify(busyLock0, never()).leaveBusy();
        inOrder.verify(busyLock1, never()).leaveBusy();
    }

    @Test
    void testLeaveBusy() {
        IgniteSpinBusyLock busyLock0 = spy(new IgniteSpinBusyLock());
        IgniteSpinBusyLock busyLock1 = spy(new IgniteSpinBusyLock());

        busyLock0.enterBusy();
        busyLock1.enterBusy();

        clearInvocations(busyLock0, busyLock1);

        leaveBusy(busyLock0, busyLock1);

        InOrder inOrder = inOrder(busyLock0, busyLock1);

        inOrder.verify(busyLock1).leaveBusy();
        inOrder.verify(busyLock0).leaveBusy();
    }

    private static ClusterService clusterService(ClusterNode localNode) {
        ClusterService clusterService = mock(ClusterService.class);
        TopologyService topologyService = mock(TopologyService.class);

        when(topologyService.localMember()).thenReturn(localNode);
        when(clusterService.topologyService()).thenReturn(topologyService);

        return clusterService;
    }
}
