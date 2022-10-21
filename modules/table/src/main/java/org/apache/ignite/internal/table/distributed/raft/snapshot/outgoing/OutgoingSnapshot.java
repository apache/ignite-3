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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataResponse;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;

/**
 * Outgoing snapshot. It corresponds to exactly one partition.
 *
 * <p>The snapshot has a lock needed for interaction with {@link SnapshotAwarePartitionDataStorage}.
 */
public class OutgoingSnapshot {
    private static final TableMessagesFactory MESSAGES_FACTORY = new TableMessagesFactory();

    private final UUID id;

    private final PartitionAccess partition;

    private final OutgoingSnapshotRegistry outgoingSnapshotRegistry;

    /**
     * Lock that is used for mutual exclusion of snapshot reading (by this class) and threads that write to the same
     * partition (currently, via {@link SnapshotAwarePartitionDataStorage}).
     */
    private final Lock rowOperationsLock = new ReentrantLock();

    /**
     * {@link RowId}s for which the corresponding rows were sent out of order (relative to the order in which this
     * snapshot sends rows), hence they must be skipped when sending rows normally.
     */
    private final Set<RowId> overwrittenRowIds = new ConcurrentHashSet<>();

    // TODO: IGNITE-17935 - manage queue size
    /**
     * Rows that need to be sent out of order (relative to the order in which this snapshot sends rows).
     * Versions inside rows are in oldest-to-newest order.
     */
    private final Queue<SnapshotMvDataResponse.ResponseEntry> outOfOrderMvData = new LinkedList<>();

    /**
     * {@link RowId} used to point (most of the time) to the last processed row. More precisely:
     *
     * <ul>
     *     <li>Before we started to read from the partition, this is equal to lowest theoretically possible
     *     {@link RowId} for this partition</li>
     *     <li>If we started to read from partition AND it is not yet exhausted, this is the last RowId that was
     *     sent in this snapshot order</li>
     *     <li>After we exhausted the partition, this is {@code null}</li>
     * </ul>
     */
    private RowId lastRowId;

    private boolean startedToReadPartition = false;

    /**
     * This becomes {@code true} as soon as we exhaust both the partition and out-of-order queue.
     */
    private boolean finished = false;

    /**
     * Creates a new instance.
     */
    public OutgoingSnapshot(UUID id, PartitionAccess partition, OutgoingSnapshotRegistry outgoingSnapshotRegistry) {
        this.id = id;
        this.partition = partition;
        this.outgoingSnapshotRegistry = outgoingSnapshotRegistry;

        lastRowId = RowId.lowestRowId(partition.key().partitionId());
    }

    /**
     * Returns the ID of this snapshot.
     */
    public UUID id() {
        return id;
    }

    /**
     * Returns the key of the corresponding partition.
     *
     * @return Partition key.
     */
    public PartitionKey partitionKey() {
        return partition.key();
    }

    /**
     * Reads a snapshot meta and returns a future with the response.
     *
     * @param metaRequest Meta request.
     */
    CompletableFuture<SnapshotMetaResponse> handleSnapshotMetaRequest(SnapshotMetaRequest metaRequest) {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17935
        return null;
    }

    /**
     * Reads chunk of partition data and returns a future with the response.
     *
     * @param request Data request.
     */
    CompletableFuture<SnapshotMvDataResponse> handleSnapshotMvDataRequest(SnapshotMvDataRequest request) {
        // TODO: IGNITE-17935 - executor?

        assert !finished;

        long totalBatchSize = 0;
        List<SnapshotMvDataResponse.ResponseEntry> batch = new ArrayList<>();

        while (true) {
            acquireLock();

            try {
                totalBatchSize = fillWithOutOfOrderRows(batch, totalBatchSize, request);

                totalBatchSize = tryProcessRowFromPartition(batch, totalBatchSize, request);

                if (exhaustedPartition() && outOfOrderMvData.isEmpty()) {
                    finished = true;
                }

                if (finished || batchIsFull(request, totalBatchSize)) {
                    break;
                }
            } finally {
                releaseLock();
            }
        }

        // We unregister itself outside the lock to avoid a deadlock, because SnapshotAwareMvPartitionStorage takes
        // locks in partitionSnapshots.lock -> snapshot.lock; if we did it under lock, we would take locks in the
        // opposite order. That's why we need finished flag and cooperation with SnapshotAwareMvPartitionStorage
        // (calling our isFinished()).
        if (finished) {
            outgoingSnapshotRegistry.unregisterOutgoingSnapshot(id);
        }

        SnapshotMvDataResponse response = MESSAGES_FACTORY.snapshotMvDataResponse()
                .rows(batch)
                .finish(finished)
                .build();

        return CompletableFuture.completedFuture(response);
    }

    private long fillWithOutOfOrderRows(
            List<SnapshotMvDataResponse.ResponseEntry> rowEntries,
            long totalBytesBefore,
            SnapshotMvDataRequest request
    ) {
        long totalBytesAfter = totalBytesBefore;

        while (totalBytesAfter < request.batchSizeHint()) {
            SnapshotMvDataResponse.ResponseEntry rowEntry = outOfOrderMvData.poll();

            if (rowEntry == null) {
                break;
            }

            rowEntries.add(rowEntry);
            totalBytesAfter += rowSizeInBytes(rowEntry.rowVersions());
        }

        return totalBytesAfter;
    }

    private long rowSizeInBytes(List<ByteBuffer> rowVersions) {
        long sum = 0;

        for (ByteBuffer buf : rowVersions) {
            if (buf != null) {
                sum += buf.remaining();
            }
        }

        return sum;
    }

    private long tryProcessRowFromPartition(List<SnapshotMvDataResponse.ResponseEntry> batch, long totalBatchSize,
            SnapshotMvDataRequest request) {
        if (batchIsFull(request, totalBatchSize) || exhaustedPartition()) {
            return totalBatchSize;
        }

        if (!startedToReadPartition) {
            lastRowId = partition.closestRowId(lastRowId);

            startedToReadPartition = true;
        } else {
            lastRowId = partition.closestRowId(lastRowId.increment());
        }

        if (!exhaustedPartition()) {
            if (!overwrittenRowIds.remove(lastRowId)) {
                List<ReadResult> rowVersions = new ArrayList<>(partition.rowVersions(lastRowId));
                Collections.reverse(rowVersions);
                SnapshotMvDataResponse.ResponseEntry rowEntry = rowEntry(lastRowId, rowVersions);

                batch.add(rowEntry);

                totalBatchSize += rowSizeInBytes(rowEntry.rowVersions());
            }
        }

        return totalBatchSize;
    }

    private boolean batchIsFull(SnapshotMvDataRequest request, long totalBatchSize) {
        return totalBatchSize >= request.batchSizeHint();
    }

    private boolean exhaustedPartition() {
        return lastRowId == null;
    }

    private SnapshotMvDataResponse.ResponseEntry rowEntry(RowId rowId, List<ReadResult> rowVersions) {
        List<ByteBuffer> buffers = new ArrayList<>();
        List<HybridTimestamp> commitTimestamps = new ArrayList<>();
        UUID transactionId = null;
        UUID commitTableId = null;
        int commitPartitionId = ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

        for (ReadResult version : rowVersions) {
            BinaryRow row = version.binaryRow();

            buffers.add(row == null ? null : row.byteBuffer());

            if (version.isWriteIntent()) {
                transactionId = version.transactionId();
                commitTableId = version.commitTableId();
                commitPartitionId = version.commitPartitionId();
            } else {
                commitTimestamps.add(version.commitTimestamp());
            }
        }

        return MESSAGES_FACTORY.responseEntry()
                .rowId(new UUID(rowId.mostSignificantBits(), rowId.leastSignificantBits()))
                .rowVersions(buffers)
                .timestamps(commitTimestamps)
                .txId(transactionId)
                .commitTableId(commitTableId)
                .commitPartitionId(commitPartitionId)
                .build();
    }

    /**
     * Reads chunk of TX states from partition and returns a future with the response.
     *
     * @param txDataRequest Data request.
     */
    CompletableFuture<SnapshotTxDataResponse> handleSnapshotTxDataRequest(SnapshotTxDataRequest txDataRequest) {
        //TODO https://issues.apache.org/jira/browse/IGNITE-17935
        return null;
    }

    /**
     * Acquires this snapshot lock.
     */
    public void acquireLock() {
        rowOperationsLock.lock();
    }

    /**
     * Releases this snapshot lock.
     */
    public void releaseLock() {
        rowOperationsLock.unlock();
    }

    /**
     * Whether this snapshot is finished (i.e. it already sent all the MV data and is not going to send anything else).
     *
     * <p>Must be called under snapshot lock.
     *
     * @return {@code true} if finished.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * Adds a {@link RowId} to the collection of IDs that need to be skipped during normal snapshot row sending.
     *
     * <p>Must be called under snapshot lock.
     *
     * @param rowId RowId to add.
     * @return {@code true} if the given RowId was added as it was not yet in the collection of IDs to skip.
     */
    public boolean addOverwrittenRowId(RowId rowId) {
        return overwrittenRowIds.add(rowId);
    }

    /**
     * Returns {@code true} if the given {@link RowId} does not interfere with the rows that this snapshot is going
     * to be sent in the normal snapshot rows sending order.
     *
     * <p>Must be called under snapshot lock.
     *
     * @param rowId RowId.
     * @return {@code true} if the given RowId is already passed by the snapshot in normal rows sending order.
     */
    public boolean alreadyPassed(RowId rowId) {
        if (!startedToReadPartition) {
            return false;
        }
        if (exhaustedPartition()) {
            return true;
        }

        return rowId.compareTo(lastRowId) <= 0;
    }

    /**
     * Enqueues a row for out-of-order sending.
     *
     * <p>Must be called under snapshot lock.
     *
     * @param rowId {@link RowId} of the row.
     * @param rowVersions Versions of the row (oldest to newest).
     */
    public void enqueueForSending(RowId rowId, List<ReadResult> rowVersions) {
        outOfOrderMvData.add(rowEntry(rowId, rowVersions));
    }
}
