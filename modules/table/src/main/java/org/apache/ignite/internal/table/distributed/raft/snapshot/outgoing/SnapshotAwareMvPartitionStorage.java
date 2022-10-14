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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.DelegatingMvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MvPartitionStorage} decorator that adds snapshot awareness. This means that writes coordinate with ongoing
 * snapshots to make sure that the writes do not interfere with the snapshots.
 */
public class SnapshotAwareMvPartitionStorage extends DelegatingMvPartitionStorage {
    private final PartitionsSnapshots partitionsSnapshots;
    private final PartitionKey partitionKey;

    /**
     * Creates a new instance.
     */
    public SnapshotAwareMvPartitionStorage(MvPartitionStorage storage, PartitionsSnapshots partitionsSnapshots, PartitionKey partitionKey) {
        super(storage);

        this.partitionsSnapshots = partitionsSnapshots;
        this.partitionKey = partitionKey;
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId,
            int commitPartitionId) throws TxIdMismatchException, StorageException {
        PartitionSnapshots partitionSnapshots = acquireReadLockOnPartitionSnapshots();

        try {
            sendRowOutOfOrderToInterferingSnapshots(rowId, partitionSnapshots);

            return super.addWrite(rowId, row, txId, commitTableId, commitPartitionId);
        } finally {
            partitionSnapshots.releaseReadLock();
        }
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        PartitionSnapshots partitionSnapshots = acquireReadLockOnPartitionSnapshots();

        try {
            sendRowOutOfOrderToInterferingSnapshots(rowId, partitionSnapshots);

            return super.abortWrite(rowId);
        } finally {
            partitionSnapshots.releaseReadLock();
        }
    }

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        PartitionSnapshots partitionSnapshots = acquireReadLockOnPartitionSnapshots();

        try {
            sendRowOutOfOrderToInterferingSnapshots(rowId, partitionSnapshots);

            super.commitWrite(rowId, timestamp);
        } finally {
            partitionSnapshots.releaseReadLock();
        }
    }

    private PartitionSnapshots acquireReadLockOnPartitionSnapshots() {
        PartitionSnapshots partitionSnapshots = partitionsSnapshots.partitionSnapshots(partitionKey);

        partitionSnapshots.acquireReadLock();

        return partitionSnapshots;
    }

    private void sendRowOutOfOrderToInterferingSnapshots(RowId rowId, PartitionSnapshots partitionSnapshots) {
        for (OutgoingSnapshot snapshot : partitionSnapshots.ongoingSnapshots()) {
            snapshot.acquireLock();

            try {
                if (snapshot.isFinished()) {
                    continue;
                }

                if (snapshot.alreadyPassed(rowId)) {
                    continue;
                }

                if (!snapshot.addOverwrittenRowId(rowId)) {
                    continue;
                }

                snapshot.enqueueForSending(rowId, rowVersions(rowId));
            } finally {
                snapshot.releaseLock();
            }
        }
    }

    private List<ReadResult> rowVersions(RowId rowId) {
        List<ReadResult> versions = new ArrayList<>();

        try (Cursor<ReadResult> cursor = scanVersions(rowId)) {
            for (ReadResult version : cursor) {
                versions.add(version);
            }
        } catch (Exception e) {
            // TODO: IGNITE-17935 - handle the exception
            throw new RuntimeException(e);
        }

        Collections.reverse(versions);

        return List.copyOf(versions);
    }

    @Override
    public void addWriteCommitted(RowId rowId, BinaryRow row, HybridTimestamp commitTimestamp) throws StorageException {
        throw new UnsupportedOperationException("This method is not supposed to be called here, this is probably a mistake");
    }

    @Override
    public void close() throws Exception {
        // TODO: IGNITE-17935 - terminate all snapshots of this partition considering correct locking to do it consistently

        super.close();
    }
}
