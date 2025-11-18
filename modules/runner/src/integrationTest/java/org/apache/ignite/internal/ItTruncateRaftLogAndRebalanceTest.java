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

package org.apache.ignite.internal;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.typesafe.config.parser.ConfigDocumentFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorageFactory.PartitionSnapshotStorageAdapter;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccess;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.junit.jupiter.api.Test;

/** Class for testing various raft log truncation and rebalancing scenarios. */
public class ItTruncateRaftLogAndRebalanceTest extends BaseTruncateRaftLogAbstractTest {
    private static final int RAFT_SNAPSHOT_INTERVAL_SECS = 5;

    @Override
    protected int initialNodes() {
        return 3;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return ConfigDocumentFactory.parseString(super.getNodeBootstrapConfigTemplate())
                .withValueText("ignite.system.properties.raftSnapshotIntervalSecs", Integer.toString(RAFT_SNAPSHOT_INTERVAL_SECS))
                .render();
    }

    @Test
    void testRestartNodeAfterAbortRebalanceAndTruncateRaftLog() throws Exception {
        createZoneAndTablePerson(ZONE_NAME, TABLE_NAME, 3, 1);

        ReplicationGroupId replicationGroupId = cluster.solePartitionId(ZONE_NAME, TABLE_NAME);

        cluster.transferLeadershipTo(0, replicationGroupId);

        insertPeopleAndAwaitTruncateRaftLogOnAllNodes(1_000, TABLE_NAME, replicationGroupId);

        startAndAbortRebalance(1, TABLE_NAME, replicationGroupId);

        // Let's restart the node with aborted rebalance.
        cluster.stopNode(1);
        cluster.startNode(1);

        // Let's check that the replication was successful.
        for (int nodeIndex = 0; nodeIndex < 3; nodeIndex++) {
            assertThat(
                    "nodeIndex=" + nodeIndex,
                    scanPeopleFromAllPartitions(nodeIndex, TABLE_NAME),
                    arrayWithSize(1_000)
            );
        }
    }

    private void startAndAbortRebalance(int nodeIndex, String tableName, ReplicationGroupId replicationGroupId) {
        NodeImpl raftNodeImpl = raftNodeImpl(nodeIndex, replicationGroupId);

        PartitionSnapshotStorageAdapter snapshotStorageAdapter = partitionSnapshotStorageAdapter(raftNodeImpl);

        PartitionMvStorageAccess mvStorageAccess = partitionMvStorageAccess(snapshotStorageAdapter);
        PartitionTxStateAccess txStateAccess = partitionTxStateAccess(snapshotStorageAdapter);

        assertThat(runAsync(() -> allOf(mvStorageAccess.startRebalance(), txStateAccess.startRebalance())), willCompleteSuccessfully());

        flushMvPartitionStorage(nodeIndex, tableName, replicationGroupId);

        assertThat(runAsync(() -> allOf(mvStorageAccess.abortRebalance(), txStateAccess.abortRebalance())), willCompleteSuccessfully());
    }

    private void flushMvPartitionStorage(int nodeIndex, String tableName, ReplicationGroupId replicationGroupId) {
        assertThat(replicationGroupId, instanceOf(PartitionGroupId.class));

        TableViewInternal tableViewInternal = tableViewInternal(nodeIndex, tableName);

        MvPartitionStorage mvPartitionStorage = mvPartitionStorage(
                tableViewInternal,
                ((PartitionGroupId) replicationGroupId).partitionId()
        );

        assertThat(
                Wrappers.unwrap(mvPartitionStorage, MvPartitionStorage.class).flush(true),
                willCompleteSuccessfully()
        );
    }

    private static PartitionSnapshotStorageAdapter partitionSnapshotStorageAdapter(NodeImpl raftNodeImpl) {
        return (PartitionSnapshotStorageAdapter) raftNodeImpl.getServiceFactory().createSnapshotStorage(
                "test",
                raftNodeImpl.getRaftOptions()
        );
    }

    private static PartitionMvStorageAccess partitionMvStorageAccess(PartitionSnapshotStorageAdapter partitionSnapshotStorageAdapter) {
        PartitionSnapshotStorage partitionSnapshotStorage = partitionSnapshotStorageAdapter.partitionSnapshotStorage();

        Collection<PartitionMvStorageAccess> mvStorageAccesses = partitionSnapshotStorage.partitionsByTableId().values();
        assertThat(mvStorageAccesses, hasSize(1));

        PartitionMvStorageAccess first = first(mvStorageAccesses);

        assertNotNull(first);

        return first;
    }

    private static PartitionTxStateAccess partitionTxStateAccess(PartitionSnapshotStorageAdapter partitionSnapshotStorageAdapter) {
        return partitionSnapshotStorageAdapter.partitionSnapshotStorage().txState();
    }

    private void insertPeopleAndAwaitTruncateRaftLogOnAllNodes(int count, String tableName, ReplicationGroupId replicationGroupId) {
        long[] beforeInsertPeopleRaftFirstLogIndexes = collectRaftFirstLogIndexes(replicationGroupId);
        assertEquals(initialNodes(), beforeInsertPeopleRaftFirstLogIndexes.length);

        insertPeople(tableName, generatePeople(count));

        await().atMost(RAFT_SNAPSHOT_INTERVAL_SECS * 2, TimeUnit.SECONDS).until(() -> {
            long[] raftFirstLogIndexes = collectRaftFirstLogIndexes(replicationGroupId);
            assertEquals(initialNodes(), raftFirstLogIndexes.length);

            return !Arrays.equals(beforeInsertPeopleRaftFirstLogIndexes, raftFirstLogIndexes);
        });
    }

    private long[] collectRaftFirstLogIndexes(ReplicationGroupId replicationGroupId) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .map(igniteImpl -> raftNodeImpl(igniteImpl, replicationGroupId))
                .mapToLong(raftNodeImpl -> raftNodeImpl.logStorage().getFirstLogIndex())
                .toArray();
    }
}
