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

package org.apache.ignite.internal.partition.replicator.raft.snapshot;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfo;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfoSerializer;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;

/** Adapter from {@link TxStatePartitionStorage} to {@link PartitionTxStateAccess}. */
public class PartitionTxStateAccessImpl implements PartitionTxStateAccess {
    private final TxStatePartitionStorage storage;

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    public PartitionTxStateAccessImpl(TxStatePartitionStorage storage) {
        this.storage = storage;
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> getAllTxMeta() {
        return storage.scan();
    }

    @Override
    public void addTxMeta(UUID txId, TxMeta txMeta) {
        storage.putForRebalance(txId, txMeta);
    }

    @Override
    public long lastAppliedIndex() {
        return storage.lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return storage.lastAppliedTerm();
    }

    @Override
    public @Nullable RaftGroupConfiguration committedGroupConfiguration() {
        return raftGroupConfigurationConverter.fromBytes(storage.committedGroupConfiguration());
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return storage.leaseInfo();
    }

    @Override
    public @Nullable PartitionSnapshotInfo snapshotInfo() {
        byte[] snapshotInfo = storage.snapshotInfo();

        return snapshotInfo == null ? null : VersionedSerialization.fromBytes(snapshotInfo, PartitionSnapshotInfoSerializer.INSTANCE);
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        return storage.startRebalance();
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        return storage.abortRebalance();
    }

    @Override
    public CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta) {
        return storage.finishRebalance(partitionMeta).thenCompose(v -> storage.flush());
    }
}
