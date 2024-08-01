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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.util.Cursor;
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

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

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
    public int tableId() {
        return partitionKey.tableId();
    }

    @Override
    public int partitionId() {
        return partitionKey.partitionId();
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return partitionStorage.runConsistently(closure);
    }

    @Override
    public void acquirePartitionSnapshotsReadLock() {
        PartitionSnapshots partitionSnapshots = getPartitionSnapshots();

        partitionSnapshots.acquireReadLock();
    }

    @Override
    public void releasePartitionSnapshotsReadLock() {
        PartitionSnapshots partitionSnapshots = getPartitionSnapshots();

        partitionSnapshots.releaseReadLock();
    }

    private PartitionSnapshots getPartitionSnapshots() {
        return partitionsSnapshots.partitionSnapshots(partitionKey);
    }

    @Override
    public CompletableFuture<Void> flush(boolean trigger) {
        return partitionStorage.flush(trigger);
    }

    @Override
    public long lastAppliedIndex() {
        return partitionStorage.lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return partitionStorage.lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        partitionStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        partitionStorage.committedGroupConfiguration(raftGroupConfigurationConverter.toBytes(config));
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, int commitTableId,
            int commitPartitionId) throws TxIdMismatchException, StorageException {
        handleSnapshotInterference(rowId);

        return partitionStorage.addWrite(rowId, row, txId, commitTableId, commitPartitionId);
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTs)
            throws TxIdMismatchException, StorageException {
        handleSnapshotInterference(rowId);

        partitionStorage.addWriteCommitted(rowId, row, commitTs);
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

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return partitionStorage.scanVersions(rowId);
    }

    /**
     * Handles the situation when snapshots are running concurrently with write operations.
     * In case if a row that is going to be changed was not yet sent in an ongoing snapshot,
     * schedule an out-of-order sending of said row.
     *
     * @param rowId Row id.
     */
    private void handleSnapshotInterference(RowId rowId) {
        PartitionSnapshots partitionSnapshots = getPartitionSnapshots();

        for (OutgoingSnapshot snapshot : partitionSnapshots.ongoingSnapshots()) {
            snapshot.acquireMvLock();

            try {
                if (snapshot.alreadyPassed(rowId)) {
                    // Row already sent.
                    continue;
                }

                if (!snapshot.addRowIdToSkip(rowId)) {
                    // Already scheduled.
                    continue;
                }

                // Collect all versions of row and schedule the send operation.
                snapshot.enqueueForSending(rowId);
            } finally {
                snapshot.releaseMvLock();
            }
        }
    }

    @Override
    public void close() {
        cleanupSnapshots();
    }

    private void cleanupSnapshots() {
        PartitionSnapshots partitionSnapshots = getPartitionSnapshots();

        partitionSnapshots.acquireReadLock();

        try {
            partitionSnapshots.ongoingSnapshots().forEach(snapshot -> partitionsSnapshots.finishOutgoingSnapshot(snapshot.id()));

            partitionsSnapshots.removeSnapshots(partitionKey);
        } finally {
            partitionSnapshots.releaseReadLock();
        }
    }

    @Override
    @TestOnly
    public MvPartitionStorage getStorage() {
        return partitionStorage;
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        return partitionStorage.scan(timestamp);
    }

    @Override
    public @Nullable GcEntry peek(HybridTimestamp lowWatermark) {
        return partitionStorage.peek(lowWatermark);
    }

    @Override
    public @Nullable BinaryRow vacuum(GcEntry entry) {
        return partitionStorage.vacuum(entry);
    }

    @Override
    public void updateLease(long leaseStartTime) {
        partitionStorage.updateLease(leaseStartTime);
    }

    @Override
    public long leaseStartTime() {
        return partitionStorage.leaseStartTime();
    }
}
