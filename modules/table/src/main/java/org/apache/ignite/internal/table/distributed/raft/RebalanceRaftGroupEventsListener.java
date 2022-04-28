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
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsPendingKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsPlannedKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsStableKey;

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
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
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
            throw new IgniteInternalException(new NodeStoppingException());
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
     * Implementation of {@link RebalanceRaftGroupEventsListener#onNewPeersConfigurationApplied(List)}
     *
     * @param peers Peers
     */
    private void doOnNewPeersConfigurationApplied(List<PeerId> peers) {
        Map<ByteArray, Entry> keys = metaStorageMgr.getAll(
                Set.of(partAssignmentsPlannedKey(partId), partAssignmentsPendingKey(partId))).join();

        Entry plannedEntry = keys.get(partAssignmentsPlannedKey(partId));
        Entry pendingEntry = keys.get(partAssignmentsPendingKey(partId));

        tblConfiguration.change(ch -> {
            List<List<ClusterNode>> assignments =
                    (List<List<ClusterNode>>) ByteUtils.fromBytes(((ExtendedTableChange) ch).assignments());
            assignments.set(partNum, ((List<ClusterNode>) ByteUtils.fromBytes(pendingEntry.value())));
            ((ExtendedTableChange) ch).changeAssignments(ByteUtils.toBytes(assignments));
        }).join();

        if (plannedEntry.value() != null) {
            if (!metaStorageMgr.invoke(If.iif(
                    revision(partAssignmentsPlannedKey(partId)).eq(plannedEntry.revision()),
                    ops(
                            put(partAssignmentsStableKey(partId), pendingEntry.value()),
                            put(partAssignmentsPendingKey(partId), plannedEntry.value()),
                            remove(partAssignmentsPlannedKey(partId)))
                            .yield(true),
                    ops().yield(false))).join().getAsBoolean()) {
                doOnNewPeersConfigurationApplied(peers);
            }
        } else {
            if (!metaStorageMgr.invoke(If.iif(
                    notExists(partAssignmentsPlannedKey(partId)),
                    ops(put(partAssignmentsStableKey(partId), pendingEntry.value()),
                            remove(partAssignmentsPendingKey(partId))).yield(true),
                    ops().yield(false))).join().getAsBoolean()) {
                doOnNewPeersConfigurationApplied(peers);
            }
        }
    }
}
