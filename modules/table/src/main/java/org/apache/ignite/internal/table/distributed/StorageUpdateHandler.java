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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.TimedBinaryRow;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AbortResult;
import org.apache.ignite.internal.storage.AbortResultStatus;
import org.apache.ignite.internal.storage.AddWriteCommittedResult;
import org.apache.ignite.internal.storage.AddWriteCommittedResultStatus;
import org.apache.ignite.internal.storage.AddWriteResult;
import org.apache.ignite.internal.storage.AddWriteResultStatus;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.replicator.PendingRows;
import org.apache.ignite.internal.thread.ThreadUtils;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/** Handler for storage updates that can be performed on processing of primary replica requests and partition replication requests. */
public class StorageUpdateHandler {
    /** Partition id. */
    private final int partitionId;

    /** Partition storage with access to MV data of a partition. */
    private final PartitionDataStorage storage;

    /** Partition index update handler. */
    private final IndexUpdateHandler indexUpdateHandler;

    /** A container for rows that were inserted, updated or removed. */
    private final PendingRows pendingRows = new PendingRows();

    /** Replication configuration. */
    private final ReplicationConfiguration replicationConfiguration;

    private boolean usePendingRowsTree = IgniteSystemProperties.getBoolean("IGNITE_USE_PENDING_ROWS_TREE");

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param storage Partition data storage.
     * @param indexUpdateHandler Partition index update handler.
     * @param replicationConfiguration Configuration for the replication.
     */
    public StorageUpdateHandler(
            int partitionId,
            PartitionDataStorage storage,
            IndexUpdateHandler indexUpdateHandler,
            ReplicationConfiguration replicationConfiguration
    ) {
        this.partitionId = partitionId;
        this.storage = storage;
        this.indexUpdateHandler = indexUpdateHandler;
        this.replicationConfiguration = replicationConfiguration;
    }

    /** Returns partition ID of the storage. */
    public int partitionId() {
        return partitionId;
    }

    /**
     * Handles single update.
     *
     * @param txId Transaction id.
     * @param rowUuid Row UUID.
     * @param commitPartitionId Commit partition id.
     * @param row Row.
     * @param trackWriteIntent If {@code true} then write intent should be tracked.
     * @param onApplication Callback on application.
     * @param commitTs Commit timestamp to use on autocommit.
     * @param lastCommitTs The timestamp of last known committed entry.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    public void handleUpdate(
            UUID txId,
            UUID rowUuid,
            ReplicationGroupId commitPartitionId,
            @Nullable BinaryRow row,
            boolean trackWriteIntent,
            @Nullable Runnable onApplication,
            @Nullable HybridTimestamp commitTs,
            @Nullable HybridTimestamp lastCommitTs,
            @Nullable List<Integer> indexIds
    ) {
        storage.runConsistently(locker -> {
            RowId rowId = new RowId(partitionId, rowUuid);

            tryProcessRow(
                    locker,
                    (PartitionGroupId) commitPartitionId,
                    rowId,
                    txId,
                    row,
                    lastCommitTs,
                    commitTs,
                    false,
                    indexIds
            );

            if (trackWriteIntent) {
                if (!usePendingRowsTree) {
                    pendingRows.addPendingRowId(txId, rowId);
                }
            }

            if (onApplication != null) {
                onApplication.run();
            }

            return null;
        });
    }

    private boolean tryProcessRow(
            Locker locker,
            PartitionGroupId commitPartitionId,
            RowId rowId,
            UUID txId,
            @Nullable BinaryRow row,
            @Nullable HybridTimestamp lastCommitTs,
            @Nullable HybridTimestamp commitTs,
            boolean useTryLock,
            @Nullable List<Integer> indexIds
    ) {
        if (useTryLock) {
            if (!locker.tryLock(rowId)) {
                return false;
            }
        } else {
            locker.lock(rowId);
        }

        performStorageCleanupIfNeeded(txId, rowId, lastCommitTs, indexIds);

        if (commitTs != null) {
            AddWriteCommittedResult result = storage.addWriteCommitted(rowId, row, commitTs);

            if (result.status() == AddWriteCommittedResultStatus.WRITE_INTENT_EXISTS) {
                throw new StorageException("Write intent already exists: [rowId={}]", rowId);
            }
        } else {
            AddWriteResult result = storage.addWrite(rowId, row, txId, commitPartitionId.objectId(), commitPartitionId.partitionId());

            if (result.status() == AddWriteResultStatus.TX_MISMATCH) {
                throw new TxIdMismatchException(result.currentWriteIntentTxId(), txId);
            }

            BinaryRow oldRow = result.previousWriteIntent();

            if (oldRow != null) {
                // Previous uncommitted row should be removed from indexes.
                tryRemovePreviousWritesIndex(rowId, oldRow, indexIds);
            }
        }

        indexUpdateHandler.addToIndexes(row, rowId, indexIds);

        return true;
    }

    /**
     * Handle multiple updates.
     *
     * @param txId Transaction id.
     * @param rowsToUpdate Collection of rows to update.
     * @param commitPartitionId Commit partition id.
     * @param trackWriteIntent If {@code true} then write intent should be tracked.
     * @param onApplication Callback on application.
     * @param commitTs Commit timestamp to use on autocommit.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    public void handleUpdateAll(
            UUID txId,
            Map<UUID, TimedBinaryRow> rowsToUpdate,
            ReplicationGroupId commitPartitionId,
            boolean trackWriteIntent,
            @Nullable Runnable onApplication,
            @Nullable HybridTimestamp commitTs,
            @Nullable List<Integer> indexIds
    ) {
        if (nullOrEmpty(rowsToUpdate)) {
            return;
        }

        Iterator<Entry<UUID, TimedBinaryRow>> it = rowsToUpdate.entrySet().iterator();
        Entry<UUID, TimedBinaryRow> lastUnprocessedEntry = it.next();

        while (lastUnprocessedEntry != null) {
            lastUnprocessedEntry = processEntriesUntilBatchLimit(
                    lastUnprocessedEntry,
                    txId,
                    trackWriteIntent,
                    commitTs,
                    (PartitionGroupId) commitPartitionId,
                    it,
                    onApplication,
                    replicationConfiguration.batchSizeBytes().value(),
                    indexIds
            );
        }
    }

    private Entry<UUID, TimedBinaryRow> processEntriesUntilBatchLimit(
            Entry<UUID, TimedBinaryRow> lastUnprocessedEntry,
            UUID txId,
            boolean trackWriteIntent,
            @Nullable HybridTimestamp commitTs,
            PartitionGroupId commitPartitionId,
            Iterator<Entry<UUID, TimedBinaryRow>> it,
            @Nullable Runnable onApplication,
            int maxBatchLength,
            @Nullable List<Integer> indexIds
    ) {
        return storage.runConsistently(locker -> {
            List<RowId> processedRowIds = new ArrayList<>();
            int batchLength = 0;
            Entry<UUID, TimedBinaryRow> entryToProcess = lastUnprocessedEntry;
            while (entryToProcess != null) {
                RowId rowId = new RowId(partitionId, entryToProcess.getKey());
                BinaryRow row = entryToProcess.getValue() == null ? null : entryToProcess.getValue().binaryRow();

                if (row != null) {
                    batchLength += row.tupleSliceLength();
                }

                if (!processedRowIds.isEmpty() && batchLength > maxBatchLength) {
                    break;
                }

                boolean rowProcessed = tryProcessRow(
                        locker,
                        commitPartitionId,
                        rowId,
                        txId,
                        row,
                        entryToProcess.getValue() == null ? null : entryToProcess.getValue().commitTimestamp(),
                        commitTs,
                        !processedRowIds.isEmpty(),
                        indexIds
                );

                if (!rowProcessed) {
                    break;
                }

                entryToProcess = it.hasNext() ? it.next() : null;
                processedRowIds.add(rowId);
            }

            if (trackWriteIntent) {
                if (!usePendingRowsTree) {
                    pendingRows.addPendingRowIds(txId, processedRowIds);
                }
            }

            if (entryToProcess == null && onApplication != null) {
                onApplication.run();
            }

            return entryToProcess;
        });
    }

    private void performStorageCleanupIfNeeded(
            UUID txId,
            RowId rowId,
            @Nullable HybridTimestamp lastCommitTs,
            @Nullable List<Integer> indexIds
    ) {
        // No previously committed value, this action might be an insert. No need to cleanup.
        if (lastCommitTs == null) {
            return;
        }

        try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
            // Okay, lastCommitTs is not null. It means that we are changing the previously committed data.
            // However, we could have previously called cleanup for the same row.
            // If the previous operation was "delete" and it was executed successfully, no data will be present in the storage.
            if (!cursor.hasNext()) {
                return;
            }

            ReadResult item = cursor.next();
            // If there is a write intent in the storage and this intent was created by a different transaction
            // then check the previous entry.
            // Otherwise exit the check - everything's fine.
            if (item.isWriteIntent() && !txId.equals(item.transactionId())) {
                if (!cursor.hasNext()) {
                    // No more data => the write intent we have is actually the first version of this row
                    // and lastCommitTs is the commit timestamp of it.
                    // Action: commit this write intent.
                    performCommitWrite(item.transactionId(), Set.of(rowId), lastCommitTs);
                    return;
                }
                // Otherwise there are other versions in the chain.
                ReadResult committedItem = cursor.next();

                // They should be regular entries, not write intents.
                assert !committedItem.isWriteIntent() : "Cannot have more than one write intent per row";

                assert lastCommitTs.compareTo(committedItem.commitTimestamp()) >= 0 :
                        "Primary commit timestamp " + lastCommitTs + " is earlier than local commit timestamp "
                                + committedItem.commitTimestamp();

                if (lastCommitTs.compareTo(committedItem.commitTimestamp()) > 0) {
                    // We see that lastCommitTs is later than the timestamp of the committed value => we need to commit the write intent.
                    // Action: commit this write intent.
                    performCommitWrite(item.transactionId(), Set.of(rowId), lastCommitTs);
                } else {
                    // lastCommitTs == committedItem.commitTimestamp()
                    // So we see a write intent from a different transaction, which was not committed on primary.
                    // Because of transaction locks we cannot have two transactions creating write intents for the same row.
                    // So if we got up to here, it means that the previous transaction was aborted,
                    // but the storage was not cleaned after it.
                    // Action: abort this write intent.
                    performAbortWrite(item.transactionId(), Set.of(rowId), indexIds);
                }
            }
        }
    }

    /**
     * Tries to remove a previous write from index.
     *
     * @param rowId Row id.
     * @param previousRow Previous write value.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    private void tryRemovePreviousWritesIndex(RowId rowId, BinaryRow previousRow, @Nullable List<Integer> indexIds) {
        try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
            if (!cursor.hasNext()) {
                return;
            }

            indexUpdateHandler.tryRemoveFromIndexes(previousRow, rowId, cursor, indexIds);
        }
    }

    /**
     * Handles the read of a write-intent.
     *
     * @param txId Transaction id.
     * @param rowId Row id.
     */
    public void handleWriteIntentRead(UUID txId, RowId rowId) {
        if (!usePendingRowsTree) {
            pendingRows.addPendingRowId(txId, rowId);
        }
    }

    /**
     * Switches write intents created by the transaction to regular values if the transaction is committed
     * or removes them if the transaction is aborted.
     *
     * @param txId Transaction id.
     * @param commit Commit flag. {@code true} if transaction is committed, {@code false} otherwise.
     * @param commitTimestamp Commit timestamp. Not {@code null} if {@code commit} is {@code true}.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    public void switchWriteIntents(
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable List<Integer> indexIds
    ) {
        switchWriteIntents(txId, commit, commitTimestamp, null, indexIds);
    }

    private static IgniteLogger LOG = Loggers.forClass(StorageUpdateHandler.class);

    /**
     * Switches write intents created by the transaction to regular values if the transaction is committed
     * or removes them if the transaction is aborted.
     *
     * @param txId Transaction id.
     * @param commit Commit flag. {@code true} if transaction is committed, {@code false} otherwise.
     * @param commitTimestamp Commit timestamp. Not {@code null} if {@code commit} is {@code true}.
     * @param onApplication On application callback.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    public void switchWriteIntents(
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable Runnable onApplication,
            @Nullable List<Integer> indexIds
    ) {
        Set<RowId> pendingRowIds;

        if (usePendingRowsTree) {
            pendingRowIds = storage.scanPendingRows(txId);
        } else {
            pendingRowIds = pendingRows.removePendingRowIds(txId);
        }

        // ThreadUtils.dumpStack(LOG, "Pending row ids for transaction {}: {} usePendingRowsTree={}", txId, pendingRowIds, usePendingRowsTree);
        // LOG.info("Pending row ids for transaction {}: {} usePendingRowsTree={}", txId, pendingRowIds, usePendingRowsTree);

        // `pendingRowIds` might be empty when we have already cleaned up the storage for this transaction,
        // for example, when primary (PartitionReplicaListener) is collocated with the raft node (PartitionListener)
        // and one of them has already processed the cleanup request, since they share the instance of this class.
        // Or the cleanup might have been done asynchronously.
        // However, we still need to run `onApplication` if it is not null, e.g. called in TxCleanupCommand handler in PartitionListener
        // to update indexes. In this case it should be executed under `runConsistently`.
        if (!pendingRowIds.isEmpty() || onApplication != null) {
            storage.runConsistently(locker -> {
                pendingRowIds.forEach(locker::lock);

                // Here we don't need to check for mismatch of the transaction that created the write intent and commits it. Since the
                // commit can happen in #handleUpdate and #handleUpdateAll.
                if (commit) {
                    performCommitWrite(txId, pendingRowIds, commitTimestamp);
                } else {
                    performAbortWrite(txId, pendingRowIds, indexIds);
                }

                if (onApplication != null) {
                    onApplication.run();
                }

                if (usePendingRowsTree) {
                    storage.trimPendingRows(txId);
                }

                return null;
            });
        }
    }

    /**
     * Commits write intents created by the provided transaction.
     *
     * <p>Transaction that created write intent is expected to commit it.</p>
     *
     * @param txId Transaction ID.
     * @param pendingRowIds Row IDs of write-intents to be committed.
     * @param commitTimestamp Commit timestamp.
     */
    private void performCommitWrite(UUID txId, Set<RowId> pendingRowIds, HybridTimestamp commitTimestamp) {
        assert commitTimestamp != null : "Commit timestamp is null: " + txId;

        pendingRowIds.forEach(rowId -> storage.commitWrite(rowId, commitTimestamp, txId));
    }

    /**
     * Aborts write intents created by the provided transaction.
     *
     * <p>Transaction that created write intent is expected to abort it.</p>
     *
     * @param txId Transaction ID.
     * @param pendingRowIds Row IDs of write-intents to be aborted.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    private void performAbortWrite(UUID txId, Set<RowId> pendingRowIds, @Nullable List<Integer> indexIds) {
        for (RowId rowId : pendingRowIds) {
            AbortResult abortResult = storage.abortWrite(rowId, txId);

            if (abortResult.status() == AbortResultStatus.TX_MISMATCH) {
                continue;
            }

            if (abortResult.status() != AbortResultStatus.SUCCESS || abortResult.previousWriteIntent() == null) {
                continue;
            }

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                indexUpdateHandler.tryRemoveFromIndexes(
                        abortResult.previousWriteIntent(),
                        rowId,
                        cursor,
                        indexIds
                );
            }
        }
    }

    /** Returns partition index update handler. */
    public IndexUpdateHandler getIndexUpdateHandler() {
        return indexUpdateHandler;
    }
}
