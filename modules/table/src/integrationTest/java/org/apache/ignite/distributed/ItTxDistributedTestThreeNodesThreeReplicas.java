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

package org.apache.ignite.distributed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Distributed transaction test using a single partition table, 3 nodes and 3 replicas.
 */
public class ItTxDistributedTestThreeNodesThreeReplicas extends TxAbstractTest {
    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedTestThreeNodesThreeReplicas(TestInfo testInfo) {
        super(testInfo);
    }

    /** {@inheritDoc} */
    @Override
    protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override
    protected int replicas() {
        return 3;
    }

    @Override
    @AfterEach
    public void after() throws Exception {
        try {
            assertTrue(IgniteTestUtils.waitForCondition(() -> assertPartitionsSame(accounts, 0), TimeUnit.SECONDS.toMillis(5)));
            assertTrue(IgniteTestUtils.waitForCondition(() -> assertPartitionsSame(customers, 0), TimeUnit.SECONDS.toMillis(5)));
        } finally {
            super.after();
        }
    }

    @Test
    public void testPrimaryReplicaDirectUpdateForExplicitTxn() throws InterruptedException {
        Peer leader = txTestCluster.getLeaderId(accounts.name());
        JraftServerImpl server = (JraftServerImpl) txTestCluster.raftServers.get(leader.consistentId()).server();
        var groupId = new TablePartitionId(accounts.tableId(), 0);

        // BLock replication messages to both replicas.
        server.blockMessages(new RaftNodeId(groupId, leader), (msg, peerId) -> {
            if (msg instanceof RpcRequests.AppendEntriesRequest) {
                RpcRequests.AppendEntriesRequest tmp = (AppendEntriesRequest) msg;

                if (tmp.entriesList() != null && !tmp.entriesList().isEmpty()) {
                    return true;
                }
            }
            return false;
        });

        assertTrue(IgniteTestUtils.waitForCondition(() -> server.blockedMessages(new RaftNodeId(groupId, leader)).size() == 2, 10000),
                "Failed to wait for blocked messages");

        ReadWriteTransactionImpl tx = (ReadWriteTransactionImpl) igniteTransactions.begin();
        CompletableFuture<Void> fut = accounts.recordView().upsertAsync(tx, makeValue(1, 100.));
        // Update must complete now despite the blocked replication protocol.
        assertTrue(IgniteTestUtils.waitForCondition(fut::isDone, 5_000), "The update future is not completed within timeout");

        server.stopBlockMessages(new RaftNodeId(groupId, leader));

        CompletableFuture<Void> commitFut = tx.commitAsync();
        assertTrue(IgniteTestUtils.waitForCondition(commitFut::isDone, 5_000), "The commit future is not completed within timeout");
    }
}
