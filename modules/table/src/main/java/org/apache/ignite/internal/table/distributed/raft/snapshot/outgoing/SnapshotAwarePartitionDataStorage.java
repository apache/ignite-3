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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lock.AutoLockup;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * {@link PartitionDataStorage} that adds snapshot awareness. This means that MV writes coordinate with ongoing
 * snapshots to make sure that the writes do not interfere with the snapshots.
 */
public class SnapshotAwarePartitionDataStorage implements PartitionDataStorage {
    private final MvPartitionStorage partitionStorage;
    private final PartitionsSnapshots partitionsSnapshots;
    private final PartitionKey partitionKey;

    /**
     * Creates a new instance.
     */
    public SnapshotAwarePartitionDataStorage(
            MvPartitionStorage partitionStorage,
            PartitionsSnapshots partitionsSnapshots,
            PartitionKey partitionKey
    ) {
        this.partitionStorage = partitionStorage;
        this.partitionsSnapshots = partitionsSnapshots;
        this.partitionKey = partitionKey;
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return partitionStorage.runConsistently(closure);
    }

    @Override
    public AutoLockup acquirePartitionSnapshotsReadLock() {
        PartitionSnapshots partitionSnapshots = getPartitionSnapshots();

        return partitionSnapshots.acquireReadLock();
    }

    private PartitionSnapshots getPartitionSnapshots() {
        return partitionsSnapshots.partitionSnapshots(partitionKey);
    }

    @Override
    public CompletableFuture<Void> flush() {
        return partitionStorage.flush();
    }

    @Override
    public long lastAppliedIndex() {
        return partitionStorage.lastAppliedIndex();
    }

    @Override
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        partitionStorage.lastAppliedIndex(lastAppliedIndex);
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId,
            int commitPartitionId) throws TxIdMismatchException, StorageException {
        handleSnapshotInterference(rowId);

        return partitionStorage.addWrite(rowId, row, txId, commitTableId, commitPartitionId);
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        handleSnapshotInterference(rowId);

        return partitionStorage.abortWrite(rowId);
    }

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        handleSnapshotInterference(rowId);

        partitionStorage.commitWrite(rowId, timestamp);
    }

    private void handleSnapshotInterference(RowId rowId) {
        PartitionSnapshots partitionSnapshots = getPartitionSnapshots();

        for (OutgoingSnapshot snapshot : partitionSnapshots.ongoingSnapshots()) {
            try (AutoLockup ignored = snapshot.acquireMvLock()) {
                if (snapshot.alreadyPassed(rowId)) {
                    continue;
                }

                if (!snapshot.addRowIdToSkip(rowId)) {
                    continue;
                }

                snapshot.enqueueForSending(rowId);
            }
        }
    }

    @Override
    public void close() throws Exception {
        cleanupSnapshots();

        partitionStorage.close();
    }

    private void cleanupSnapshots() {
        PartitionSnapshots partitionSnapshots = getPartitionSnapshots();

        try (AutoLockup ignored = partitionSnapshots.acquireReadLock()) {
            partitionSnapshots.ongoingSnapshots().forEach(snapshot -> partitionsSnapshots.finishOutgoingSnapshot(snapshot.id()));

            partitionsSnapshots.removeSnapshots(partitionKey);
        }
    }

    @Override
    @TestOnly
    public MvPartitionStorage getStorage() {
        return partitionStorage;
    }
}
