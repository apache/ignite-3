/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.revision;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;

/**
 * Listener for the raft group events, which must provide correct error handling of rebalance process
 * and start new rebalance after the current one finished.
 */
public class RebalanceRaftGroupEventsListener implements RaftGroupEventsListener {

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Table configuration instance. */
    private final TableConfiguration tblConfiguration;

    /** Unique partition id. */
    private final String partId;

    /** Partition number. */
    private final int partNum;

    /** Busy lock of parent component for synchronous stop. */
    private IgniteSpinBusyLock busyLock;

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param tblConfiguration Table configuration.
     * @param partId Partition id.
     * @param partNum Partition number.
     */
    public RebalanceRaftGroupEventsListener(
            MetaStorageManager metaStorageMgr,
            TableConfiguration tblConfiguration,
            String partId,
            int partNum,
            IgniteSpinBusyLock busyLock) {
        this.metaStorageMgr = metaStorageMgr;
        this.tblConfiguration = tblConfiguration;
        this.partId = partId;
        this.partNum = partNum;
        this.busyLock = busyLock;
    }

    /** {@inheritDoc} */
    @Override
    public void onLeaderElected() {
        // TODO: IGNITE-16800 implement this method
    }

    /** {@inheritDoc} */
    @Override
    public void onNewPeersConfigurationApplied(List<PeerId> peers) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            doOnNewPeersConfigurationApplied(peers);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onReconfigurationError(Status status) {
        // TODO: IGNITE-14873 implement this method
    }

    /**
     * Implementation of {@link RebalanceRaftGroupEventsListener#onNewPeersConfigurationApplied(List)}.
     *
     * @param peers Peers
     */
    private void doOnNewPeersConfigurationApplied(List<PeerId> peers) {
        Map<ByteArray, Entry> keys = metaStorageMgr.getAll(
                Set.of(
                        plannedPartAssignmentsKey(partId),
                        pendingPartAssignmentsKey(partId),
                        stablePartAssignmentsKey(partId))).join();

        Entry plannedEntry = keys.get(plannedPartAssignmentsKey(partId));

        List<ClusterNode> appliedPeers = resolveClusterNodes(peers,
                keys.get(pendingPartAssignmentsKey(partId)).value(), keys.get(stablePartAssignmentsKey(partId)).value());

        tblConfiguration.change(ch -> {
            List<List<ClusterNode>> assignments =
                    (List<List<ClusterNode>>) ByteUtils.fromBytes(((ExtendedTableChange) ch).assignments());
            assignments.set(partNum, appliedPeers);
            ((ExtendedTableChange) ch).changeAssignments(ByteUtils.toBytes(assignments));
        }).join();

        if (plannedEntry.value() != null) {
            if (!metaStorageMgr.invoke(If.iif(
                    revision(plannedPartAssignmentsKey(partId)).eq(plannedEntry.revision()),
                    ops(
                            put(stablePartAssignmentsKey(partId), ByteUtils.toBytes(appliedPeers)),
                            put(pendingPartAssignmentsKey(partId), plannedEntry.value()),
                            remove(plannedPartAssignmentsKey(partId)))
                            .yield(true),
                    ops().yield(false))).join().getAsBoolean()) {
                doOnNewPeersConfigurationApplied(peers);
            }
        } else {
            if (!metaStorageMgr.invoke(If.iif(
                    notExists(plannedPartAssignmentsKey(partId)),
                    ops(put(stablePartAssignmentsKey(partId), ByteUtils.toBytes(appliedPeers)),
                            remove(pendingPartAssignmentsKey(partId))).yield(true),
                    ops().yield(false))).join().getAsBoolean()) {
                doOnNewPeersConfigurationApplied(peers);
            }
        }
    }

    private static List<ClusterNode> resolveClusterNodes(
            List<PeerId> peers, byte[] pendingAssignments, byte[] stableAssignments) {
        Map<NetworkAddress, ClusterNode> resolveRegistry = new HashMap<>();

        if (pendingAssignments != null) {
            ((List<ClusterNode>) ByteUtils.fromBytes(pendingAssignments)).forEach(n -> resolveRegistry.put(n.address(), n));
        }

        if (stableAssignments != null) {
            ((List<ClusterNode>) ByteUtils.fromBytes(stableAssignments)).forEach(n -> resolveRegistry.put(n.address(), n));
        }

        List<ClusterNode> resolvedNodes = new ArrayList<>(peers.size());

        for (PeerId p : peers) {
            var addr = NetworkAddress.from(p.getEndpoint().getIp() + ":" + p.getEndpoint().getPort());

            if (resolveRegistry.containsKey(addr)) {
                resolvedNodes.add(resolveRegistry.get(addr));
            } else {
                throw new IgniteInternalException("Can't find appropriate cluster node for raft group peer: " + p);
            }
        }

        return resolvedNodes;
    }
}
