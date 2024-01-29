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
import static org.apache.ignite.internal.index.IndexManagementUtils.extractIndexIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.extractPartitionIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.inProgressBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
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
import static org.mockito.Mockito.mock;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

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
}
