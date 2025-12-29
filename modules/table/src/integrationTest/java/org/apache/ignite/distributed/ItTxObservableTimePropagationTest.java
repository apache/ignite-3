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

import static org.apache.ignite.distributed.ItTxTestCluster.NODE_PORT_BASE;
import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.table.TxInfrastructureTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests if commit timestamp and safe timestamp monotonically grow on leader change.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "1000000")
public class ItTxObservableTimePropagationTest extends TxInfrastructureTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItTxObservableTimePropagationTest.class);

    private static final long CLIENT_FROZEN_PHYSICAL_TIME = 3000;

    private static final int CLIENT_PORT = NODE_PORT_BASE - 1;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxObservableTimePropagationTest(TestInfo testInfo) {
        super(testInfo);
    }

    @Override
    protected int nodes() {
        return 3;
    }

    @Override
    protected int replicas() {
        return 3;
    }

    @Override
    protected HybridClock createClock(InternalClusterNode node) {
        int idx = NODE_PORT_BASE - node.address().port() + 1;

        // Physical time is frozen.
        return new TestHybridClock(
                () -> node.address().port() == CLIENT_PORT ? CLIENT_FROZEN_PHYSICAL_TIME : CLIENT_FROZEN_PHYSICAL_TIME + 1000L * idx);
    }

    @Override
    protected long getSafeTimePropagationTimeout() {
        return 300_000;
    }

    @Test
    public void testImplicitObservableTimePropagation() {
        RecordView<Tuple> view = accounts.recordView();
        view.upsert(null, makeValue(1, 100.0));
        List<TxStateMeta> states = txTestCluster.states();
        assertEquals(1, states.size());
        HybridTimestamp commitTs = states.get(0).commitTimestamp();

        LOG.info("commitTs={}", commitTs);

        assertNotNull(commitTs);
        assertEquals(commitTs, timestampTracker.get());

        assertTrue(commitTs.compareTo(new HybridTimestamp(CLIENT_FROZEN_PHYSICAL_TIME, 0)) > 0, "Observable timestamp should be advanced");

        ReplicationGroupId part = replicationGroupId(accounts, 0);

        NodeImpl[] handle = {null};
        NodeImpl[] leader = {null};

        txTestCluster.raftServers().values().stream().map(Loza::server).forEach(s -> {
            JraftServerImpl srv = (JraftServerImpl) s;
            srv.localNodes().stream().map(srv::raftGroupService).forEach(raftGroupService -> {
                NodeImpl raftNode = (NodeImpl) raftGroupService.getRaftNode();

                // Skip other table.
                if (!raftNode.getNodeId().getGroupId().equals(part.toString())) {
                    return;
                }

                // Ignore current leader.
                if (handle[0] == null && !raftNode.getLeaderId().equals(raftNode.getNodeId().getPeerId())) {
                    handle[0] = raftNode;
                }

                if (raftNode.getLeaderId().equals(raftNode.getNodeId().getPeerId())) {
                    leader[0] = raftNode;
                }

                var fsm = (JraftServerImpl.DelegatingStateMachine) raftNode.getOptions().getFsm();

                try {
                    assertTrue(IgniteTestUtils.waitForCondition(() -> currentSafeTime(fsm).equals(commitTs), 30_000),
                            "Safe ts is not propagated to replica " + raftNode.getNodeId());
                } catch (InterruptedException e) {
                    fail("Unexpected interrupt");
                }

                LOG.info("DBG: node={}, group={}, safeTs={}", raftNode.getNodeId(), raftNode.getGroupId(), currentSafeTime(fsm));
            });
        });

        assertNotEquals(leader[0].getNodeId(), handle[0].getNodeId());

        Status status = leader[0].transferLeadershipTo(handle[0].getNodeId().getPeerId());
        assertTrue(status.isOk());

        view.upsert(null, makeValue(1, 200.0));

        states = txTestCluster.states();
        assertEquals(2, states.size());

        // Find second transaction.
        HybridTimestamp commitTs2 = states.stream().filter(s -> !s.commitTimestamp().equals(commitTs)).reduce((f, s) -> s).get()
                .commitTimestamp();
        assertNotNull(commitTs2);

        LOG.info("After leader change: commitTs={}", commitTs2);

        assertTrue(commitTs2.compareTo(commitTs) > 0, "Invalid safe time");
    }

    private static HybridTimestamp currentSafeTime(DelegatingStateMachine fsm) {
        ZonePartitionRaftListener zonePartitionRaftListener = (ZonePartitionRaftListener) fsm.getListener();
        return zonePartitionRaftListener.currentSafeTime();
    }
}
