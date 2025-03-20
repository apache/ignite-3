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
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Small abstractions for TX storages that includes only methods, mandatory for the snapshot storage.
 *
 * @see PartitionMvStorageAccess
 */
public interface PartitionTxStateAccess {
    /**
     * Creates a cursor to scan all meta of transactions.
     *
     * <p>All metas that exist at the time the method is called will be returned, in transaction ID order as an unsigned 128 bit integer.
     */
    Cursor<IgniteBiTuple<UUID, TxMeta>> getAllTxMeta();

    /**
     * Adds transaction meta.
     *
     * @param txId Transaction ID.
     * @param txMeta Transaction meta.
     */
    void addTxMeta(UUID txId, TxMeta txMeta);

    /** Returns the last applied index of this storage. */
    long lastAppliedIndex();

    /** Returns the last applied term of this storage. */
    long lastAppliedTerm();

    /**
     * Returns committed RAFT group configuration corresponding to the write command with the highest applied index, {@code null} if it was
     * never saved.
     */
    @Nullable RaftGroupConfiguration committedGroupConfiguration();

    /**
     * Returns the last saved lease information or {@code null} if it was never saved.
     */
    @Nullable LeaseInfo leaseInfo();

    /**
     * Returns the last saved snapshot information or {@code null} if it was never saved.
     */
    @Nullable PartitionSnapshotInfo snapshotInfo();

    /**
     * Prepares the TX storage for rebalance with the same guarantees and requirements as {@link PartitionMvStorageAccess#startRebalance}.
     */
    CompletableFuture<Void> startRebalance();

    /**
     * Aborts an ongoing TX storage rebalance rebalance with the same guarantees and requirements as
     * {@link PartitionMvStorageAccess#abortRebalance}.
     */
    CompletableFuture<Void> abortRebalance();

    /**
     * Completes rebalancing of the TX storage with the same guarantees and requirements as
     * {@link PartitionMvStorageAccess#finishRebalance}.
     */
    CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta);
}
