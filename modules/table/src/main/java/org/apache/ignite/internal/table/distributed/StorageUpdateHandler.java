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
import org.apache.ignite.internal.datareplication.network.TimedBinaryRow;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.PendingRows;
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

    /** Storage updater configuration. */
    private final StorageUpdateConfiguration storageUpdateConfiguration;

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param storage Partition data storage.
     * @param indexUpdateHandler Partition index update handler.
     * @param storageUpdateConfiguration Configuration for the storage update handler.
     */
    public StorageUpdateHandler(
            int partitionId,
            PartitionDataStorage storage,
            IndexUpdateHandler indexUpdateHandler,
            StorageUpdateConfiguration storageUpdateConfiguration
    ) {
        this.partitionId = partitionId;
        this.storage = storage;
        this.indexUpdateHandler = indexUpdateHandler;
        this.storageUpdateConfiguration = storageUpdateConfiguration;
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
            TablePartitionId commitPartitionId,
            @Nullable BinaryRow row,
            boolean trackWriteIntent,
            @Nullable Runnable onApplication,
            @Nullable HybridTimestamp commitTs,
            @Nullable HybridTimestamp lastCommitTs,
            @Nullable List<Integer> indexIds
    ) {
        storage.runConsistently(locker -> {
            int commitTblId = commitPartitionId.tableId();
            int commitPartId = commitPartitionId.partitionId();
            RowId rowId = new RowId(partitionId, rowUuid);

            tryProcessRow(
                    locker,
                    commitTblId,
                    commitPartId,
                    rowId,
                    txId,
                    row,
                    lastCommitTs,
                    commitTs,
                    false,
                    indexIds
            );

            if (trackWriteIntent) {
                pendingRows.addPendingRowId(txId, rowId);
            }

            if (onApplication != null) {
                onApplication.run();
            }

            return null;
        });
    }

    private boolean tryProcessRow(
            Locker locker,
            int commitTblId,
            int commitPartId,
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
            storage.addWriteCommitted(rowId, row, commitTs);
        } else {
            BinaryRow oldRow = storage.addWrite(rowId, row, txId, commitTblId, commitPartId);

            if (oldRow != null) {
                assert commitTs == null : String.format("Expecting explicit txn: [txId=%s]", txId);
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
            TablePartitionId commitPartitionId,
            boolean trackWriteIntent,
            @Nullable Runnable onApplication,
            @Nullable HybridTimestamp commitTs,
            @Nullable List<Integer> indexIds
    ) {
        if (nullOrEmpty(rowsToUpdate)) {
            return;
        }

        int commitTblId = commitPartitionId.tableId();
        int commitPartId = commitPartitionId.partitionId();

        Iterator<Entry<UUID, TimedBinaryRow>> it = rowsToUpdate.entrySet().iterator();
        Entry<UUID, TimedBinaryRow> lastUnprocessedEntry = it.next();

        while (lastUnprocessedEntry != null) {
            lastUnprocessedEntry = processEntriesUntilBatchLimit(
                    lastUnprocessedEntry,
                    txId,
                    trackWriteIntent,
                    commitTs,
                    commitTblId,
                    commitPartId,
                    it,
                    onApplication,
                    storageUpdateConfiguration.batchByteLength().value(),
                    indexIds
            );
        }
    }

    private Entry<UUID, TimedBinaryRow> processEntriesUntilBatchLimit(
            Entry<UUID, TimedBinaryRow> lastUnprocessedEntry,
            UUID txId,
            boolean trackWriteIntent,
            @Nullable HybridTimestamp commitTs,
            int commitTblId,
            int commitPartId,
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
                        commitTblId,
                        commitPartId,
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
                pendingRows.addPendingRowIds(txId, processedRowIds);
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
        pendingRows.addPendingRowId(txId, rowId);
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
        Set<RowId> pendingRowIds = pendingRows.removePendingRowIds(txId);

        // `pendingRowIds` might be empty when we have already cleaned up the storage for this transaction,
        // for example, when primary (PartitionReplicaListener) is collocated with the raft node (PartitionListener)
        // and one of them has already processed the cleanup request, since they share the instance of this class.
        // Or the cleanup might have been done asynchronously.
        // However, we still need to run `onApplication` if it is not null, e.g. called in TxCleanupCommand handler in PartitionListener
        // to update indexes. In this case it should be executed under `runConsistently`.
        if (!pendingRowIds.isEmpty() || onApplication != null) {
            storage.runConsistently(locker -> {
                pendingRowIds.forEach(locker::lock);

                if (commit) {
                    performCommitWrite(txId, pendingRowIds, commitTimestamp);
                } else {
                    performAbortWrite(txId, pendingRowIds, indexIds);
                }

                if (onApplication != null) {
                    onApplication.run();
                }

                return null;
            });
        }
    }

    /**
     * Commit write intents created by the provided transaction.
     *
     * @param txId Transaction id
     * @param pendingRowIds Row ids of write-intents to be committed.
     * @param commitTimestamp Commit timestamp.
     */
    private void performCommitWrite(UUID txId, Set<RowId> pendingRowIds, HybridTimestamp commitTimestamp) {
        assert commitTimestamp != null : "Commit timestamp is null";

        // Please note: `pendingRowIds` might not contain the complete set of rows that were changed by this transaction:
        // Pending rows are stored in memory and will be lost in case a node restarts.
        // This method might be called by a write intent resolving transaction that will find only those rows that it needs itself.
        List<RowId> rowIds = new ArrayList<>();

        for (RowId pendingRowId : pendingRowIds) {

            // Here we check that the write intent we are going to commit still belongs to the provided transaction.
            //
            // This check is required to cover the following case caused by asynchronous cleanup of write intents:
            // 1. RO Transaction A sees a write intent for a row1, resolves it and schedules a cleanup for it.
            // 2. RW Transaction B sees the same write intent for a row1, resolves it and schedules a cleanup for it.
            // This cleanup action finishes first. Then Transaction B adds its own write intent for the row1.
            // 3. Transaction A starts executing the cleanup action.
            // Without this check it would commit the write intent from a different transaction.
            //
            // This is just a workaround. The proper fix is to check the transaction id for the row in the storage.
            // TODO: https://issues.apache.org/jira/browse/IGNITE-20347 to check transaction id in the storage
            ReadResult result = storage.getStorage().read(pendingRowId, HybridTimestamp.MAX_VALUE);
            if (result.isWriteIntent() && txId.equals(result.transactionId())) {
                // In case of an asynchronous cleanup of write intents, we might get into a situation when some of the
                // write intents were already cleaned up. In this case, we just ignore them.
                rowIds.add(pendingRowId);
            }
        }

        rowIds.forEach(rowId -> storage.commitWrite(rowId, commitTimestamp));
    }

    /**
     * Abort write intents created by the provided transaction.
     *
     * @param txId Transaction id
     * @param pendingRowIds Row ids of write-intents to be aborted.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    private void performAbortWrite(UUID txId, Set<RowId> pendingRowIds, @Nullable List<Integer> indexIds) {
        List<RowId> rowIds = new ArrayList<>();

        for (RowId rowId : pendingRowIds) {
            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                if (!cursor.hasNext()) {
                    continue;
                }

                ReadResult item = cursor.next();

                // TODO: https://issues.apache.org/jira/browse/IGNITE-20124 Prevent double storage updates within primary
                if (item.isWriteIntent()) {
                    // We are aborting only those write intents that belong to the provided transaction.
                    // TODO: https://issues.apache.org/jira/browse/IGNITE-20347 to check transaction id in the storage
                    if (!txId.equals(item.transactionId())) {
                        continue;
                    }
                    rowIds.add(rowId);

                    BinaryRow rowToRemove = item.binaryRow();

                    if (rowToRemove == null) {
                        continue;
                    }

                    indexUpdateHandler.tryRemoveFromIndexes(rowToRemove, rowId, cursor, indexIds);
                }
            }
        }

        rowIds.forEach(storage::abortWrite);
    }

    /** Returns partition index update handler. */
    public IndexUpdateHandler getIndexUpdateHandler() {
        return indexUpdateHandler;
    }
}
