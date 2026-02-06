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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
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
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Handler for storage updates that can be performed on processing of primary replica requests and partition replication requests. */
public class StorageUpdateHandler {
    private static final IgniteLogger LOG = Loggers.forClass(StorageUpdateHandler.class);

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

    /** Partition modification counter. */
    private final PartitionModificationCounter modificationCounter;

    /** Transaction manager to retrieve labels for logging. */
    private final TxManager txManager;

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param storage Partition data storage.
     * @param indexUpdateHandler Partition index update handler.
     * @param replicationConfiguration Configuration for the replication.
     * @param modificationCounter Partition modification counter.
     */
    public StorageUpdateHandler(
            int partitionId,
            PartitionDataStorage storage,
            IndexUpdateHandler indexUpdateHandler,
            ReplicationConfiguration replicationConfiguration,
            PartitionModificationCounter modificationCounter
    ) {
        this(partitionId, storage, indexUpdateHandler, replicationConfiguration, modificationCounter, null);
    }

    /**
     * The constructor.
     *
     * @param partitionId Partition id.
     * @param storage Partition data storage.
     * @param indexUpdateHandler Partition index update handler.
     * @param replicationConfiguration Configuration for the replication.
     * @param modificationCounter Partition modification counter.
     * @param txManager tx manager to retrieve label for logging.
     */
    public StorageUpdateHandler(
            int partitionId,
            PartitionDataStorage storage,
            IndexUpdateHandler indexUpdateHandler,
            ReplicationConfiguration replicationConfiguration,
            PartitionModificationCounter modificationCounter,
            @Nullable TxManager txManager
    ) {
        this.partitionId = partitionId;
        this.storage = storage;
        this.indexUpdateHandler = indexUpdateHandler;
        this.replicationConfiguration = replicationConfiguration;
        this.modificationCounter = modificationCounter;
        this.txManager = txManager;
    }

    /** Returns partition ID of the storage. */
    public int partitionId() {
        return partitionId;
    }

    /**
     * Starts the handler.
     *
     * @param onNodeRecovery {@code true} if called on node recovery, {@code false} otherwise.
     */
    public void start(boolean onNodeRecovery) {
        if (onNodeRecovery) {
            recoverPendingRows();
        }
    }

    private void recoverPendingRows() {
        long startNanos = System.nanoTime();

        int count = 0;
        try (Cursor<RowId> writeIntentRowIds = storage.getStorage().scanWriteIntents()) {
            for (RowId rowId : writeIntentRowIds) {
                // TODO: https://issues.apache.org/jira/browse/IGNITE-27234 - only read row metadata.
                ReadResult result = storage.getStorage().read(rowId, HybridTimestamp.MAX_VALUE);

                if (result.isWriteIntent()) {
                    UUID txId = result.transactionId();
                    assert txId != null : "Transaction ID is null for a write intent [rowId=" + rowId + "]";

                    pendingRows.addPendingRowId(txId, rowId);
                }

                count++;
            }
        }

        if (count != 0 && LOG.isInfoEnabled()) {
            LOG.info(
                    "Recovered pending rows [tableId={}, partitionId={}, count={}, duration={}ms]",
                    storage.tableId(),
                    storage.partitionId(),
                    count,
                    NANOSECONDS.toMillis(System.nanoTime() - startNanos)
            );
        }
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
        // Either we track write intents for later commit (2PC) or commit immediately with timestamp (1PC).
        assert trackWriteIntent || commitTs != null : "either trackWriteIntent must be true or commitTs must be non-null";

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
                pendingRows.addPendingRowId(txId, rowId);
            } else {
                modificationCounter.updateValue(1, commitTs);
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

        if (commitTs != null) {
            performAddWriteCommittedWithCleanup(rowId, row, commitTs, txId, lastCommitTs, indexIds);
        } else {
            performAddWriteWithCleanup(rowId, row, txId, commitPartitionId, lastCommitTs, indexIds);
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
        // Either we track write intents for later commit (2PC) or commit immediately with timestamp (1PC).
        assert trackWriteIntent || commitTs != null : "either trackWriteIntent must be true or commitTs must be non-null";
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

                if (!processedRowIds.isEmpty() && (locker.shouldRelease() || batchLength > maxBatchLength)) {
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
                pendingRows.addPendingRowIds(txId, processedRowIds);
            } else {
                modificationCounter.updateValue(processedRowIds.size(), commitTs);
            }
            if (entryToProcess == null && onApplication != null) {
                onApplication.run();
            }

            return entryToProcess;
        });
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
        assert !(commit && commitTimestamp == null) : "Commit timestamp cant be null when tx is set to commit: " + txId;

        // `pendingRowIds` might be empty when we have already cleaned up the storage for this transaction,
        // for example, when primary (PartitionReplicaListener) is collocated with the raft node (PartitionListener)
        // and one of them has already processed the cleanup request, since they share the instance of this class.
        // Or the cleanup might have been done asynchronously.
        // However, we still need to run `onApplication` if it is not null, e.g. called in TxCleanupCommand handler in PartitionListener
        // to update indexes. In this case it should be executed under `runConsistently`.
        if (!pendingRowIds.isEmpty() || onApplication != null) {
            Iterator<RowId> pendingRowIdsIterator = pendingRowIds.iterator();
            boolean finished = false;
            while (!finished) {
                finished = storage.runConsistently(locker -> {
                    int modificationsCount = 0;
                    boolean shouldRelease = false;
                    while (pendingRowIdsIterator.hasNext()) {
                        RowId rowId = pendingRowIdsIterator.next();
                        locker.lock(rowId);

                        // Here we don't need to check for mismatch of the transaction that created the write intent and commits it.
                        // Since the commit can happen in #handleUpdate and #handleUpdateAll.
                        if (commit) {
                            storage.commitWrite(rowId, commitTimestamp, txId);
                            modificationsCount++;
                        } else {
                            performAbortWrite(txId, rowId, indexIds);
                        }

                        shouldRelease = locker.shouldRelease();
                        if (shouldRelease) {
                            break;
                        }
                    }

                    if (commit) {
                        modificationCounter.updateValue(modificationsCount, commitTimestamp);
                    }

                    if (shouldRelease) {
                        return false;
                    }

                    if (onApplication != null) {
                        onApplication.run();
                    }

                    return true;
                });
            }
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

        if (!pendingRowIds.isEmpty()) {
            modificationCounter.updateValue(pendingRowIds.size(), commitTimestamp);
        }
    }

    /**
     * Aborts write intents created by the provided transaction.
     *
     * <p>Transaction that created write intent is expected to abort it.</p>
     *
     * @param txId Transaction ID.
     * @param rowId Row ID of write-intent to be aborted.
     * @param indexIds IDs of indexes that will need to be updated, {@code null} for all indexes.
     */
    private void performAbortWrite(UUID txId, RowId rowId, @Nullable List<Integer> indexIds) {
        AbortResult abortResult = storage.abortWrite(rowId, txId);

        if (abortResult.status() == AbortResultStatus.TX_MISMATCH) {
            return;
        }

        if (abortResult.status() != AbortResultStatus.SUCCESS || abortResult.previousWriteIntent() == null) {
            return;
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

    /** Returns partition index update handler. */
    public IndexUpdateHandler getIndexUpdateHandler() {
        return indexUpdateHandler;
    }

    /**
     * Performs add of the committed row version. If a write intent is detected on the first attempt and {@code lastCommitTs} is not
     * {@code null}, it will be cleared before the second attempt. Otherwise, {@link StorageException} will be thrown.
     */
    private void performAddWriteCommittedWithCleanup(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTs,
            UUID txId,
            @Nullable HybridTimestamp lastCommitTs,
            @Nullable List<Integer> indexIds
    ) {
        AddWriteCommittedResult result = storage.addWriteCommitted(rowId, row, commitTs);

        if (result.status() == AddWriteCommittedResultStatus.WRITE_INTENT_EXISTS) {
            if (lastCommitTs == null) {
                throw new StorageException("Write intent exists: [rowId={}]", rowId);
            }

            UUID wiTxId = result.currentWriteIntentTxId();

            performWriteIntentCleanup(rowId, txId, wiTxId, lastCommitTs, result.latestCommitTimestamp(), indexIds);

            result = storage.addWriteCommitted(rowId, row, commitTs);

            assert result.status() == AddWriteCommittedResultStatus.SUCCESS : "rowId=" + rowId + ", result=" + result;
        }
    }

    /**
     * Performs add write intent. If a write intent from another transaction is detected on the first attempt and {@code lastCommitTs} is
     * not {@code null}, it will be cleared before the second attempt. Otherwise, {@link TxIdMismatchException} will be thrown.
     */
    private void performAddWriteWithCleanup(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            PartitionGroupId commitPartitionId,
            @Nullable HybridTimestamp lastCommitTs,
            @Nullable List<Integer> indexIds
    ) {
        AddWriteResult result = performAddWrite(rowId, row, txId, commitPartitionId, indexIds);

        if (result.status() == AddWriteResultStatus.TX_MISMATCH) {
            UUID wiTxId = result.currentWriteIntentTxId();

            if (lastCommitTs == null) {

                String formattedMessage = txManager != null ? format(
                        "Mismatched transaction id [expectedTxId={}, conflictingTxId={}]",
                        formatTxInfo(wiTxId, txManager),
                        formatTxInfo(txId, txManager)) : null;

                throw new TxIdMismatchException(wiTxId, txId, formattedMessage);
            }

            performWriteIntentCleanup(rowId, txId, wiTxId, lastCommitTs, result.latestCommitTimestamp(), indexIds);

            result = performAddWrite(rowId, row, txId, commitPartitionId, indexIds);

            assert result.status() == AddWriteResultStatus.SUCCESS : "rowId=" + rowId + ", result=" + result;
        }
    }

    /** Performs add write intent, if successful and there is a previous intent (for same transaction) will clear the indexes from it. */
    private AddWriteResult performAddWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            PartitionGroupId commitPartitionId,
            @Nullable List<Integer> indexIds
    ) {
        AddWriteResult result = storage.addWrite(rowId, row, txId, commitPartitionId.objectId(), commitPartitionId.partitionId());

        if (result.status() == AddWriteResultStatus.SUCCESS && result.previousWriteIntent() != null) {
            tryRemovePreviousWritesIndex(rowId, result.previousWriteIntent(), indexIds);
        }

        return result;
    }

    /** Performs cleanup of a write intent created by another transaction. */
    private void performWriteIntentCleanup(
            RowId rowId,
            UUID txId,
            UUID writeIntentTxId,
            HybridTimestamp lastCommitTs,
            @Nullable HybridTimestamp latestCommittedTs,
            @Nullable List<Integer> indexIds
    ) {
        assert !txId.equals(writeIntentTxId) : String.format("Transactions must not match: [rowId=%s, txId=%s]", rowId, txId);

        if (latestCommittedTs == null) {
            // No more data => the write intent we have is actually the first version of this row
            // and lastCommitTs is the commit timestamp of it.
            // Action: commit this write intent.
            performCommitWrite(writeIntentTxId, Set.of(rowId), lastCommitTs);
            return;
        }

        if (lastCommitTs.compareTo(latestCommittedTs) < 0) {
            throw new IgniteException(
                    Common.INTERNAL_ERR,
                    String.format("Primary commit timestamp %s is earlier than local commit timestamp %s", lastCommitTs, latestCommittedTs)
            );
        }

        if (lastCommitTs.compareTo(latestCommittedTs) > 0) {
            // We see that lastCommitTs is later than the timestamp of the committed value => we need to commit the write intent.
            // Action: commit this write intent.
            performCommitWrite(writeIntentTxId, Set.of(rowId), lastCommitTs);
        } else {
            // lastCommitTs == latestCommittedTs
            // So we see a write intent from a different transaction, which was not committed on primary.
            // Because of transaction locks we cannot have two transactions creating write intents for the same row.
            // So if we got up to here, it means that the previous transaction was aborted,
            // but the storage was not cleaned after it.
            // Action: abort this write intent.
            performAbortWrite(writeIntentTxId, rowId, indexIds);
        }
    }

    /**
     * Erases volatile state for a transaction to simulate node restart in tests.
     * This creates a state where write intents are persisted in storage but no information
     * about them exists in memory.
     */
    @TestOnly
    public void eraseVolatileState(UUID txId) {
        this.pendingRows.removePendingRowIds(txId);
    }
}
