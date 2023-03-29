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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_UNEXPECTED_STATE_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.CommittedConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TablePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionListener.class);

    /** Partition storage with access to MV data of a partition. */
    private final PartitionDataStorage storage;

    /** Handler that processes storage updates. */
    private final StorageUpdateHandler storageUpdateHandler;

    /** Storage of transaction metadata. */
    private final TxStateStorage txStateStorage;

    /** Rows that were inserted, updated or removed. */
    private final HashMap<UUID, Set<RowId>> txsPendingRowIds = new HashMap<>();

    /** Safe time tracker. */
    private final PendingComparableValuesTracker<HybridTimestamp> safeTime;

    /** Storage index tracker. */
    private final PendingComparableValuesTracker<Long> storageIndexTracker;

    /**
     * The constructor.
     *
     * @param partitionDataStorage The storage.
     * @param safeTime Safe time tracker.
     * @param storageIndexTracker Storage index tracker.
     */
    public PartitionListener(
            PartitionDataStorage partitionDataStorage,
            StorageUpdateHandler storageUpdateHandler,
            TxStateStorage txStateStorage,
            PendingComparableValuesTracker<HybridTimestamp> safeTime,
            PendingComparableValuesTracker<Long> storageIndexTracker
    ) {
        this.storage = partitionDataStorage;
        this.storageUpdateHandler = storageUpdateHandler;
        this.txStateStorage = txStateStorage;
        this.safeTime = safeTime;
        this.storageIndexTracker = storageIndexTracker;

        // TODO: IGNITE-18502 Implement a pending update storage
        try (PartitionTimestampCursor cursor = partitionDataStorage.getStorage().scan(HybridTimestamp.MAX_VALUE)) {
            if (cursor != null) {
                while (cursor.hasNext()) {
                    ReadResult readResult = cursor.next();

                    if (readResult.isWriteIntent()) {
                        txsPendingRowIds.computeIfAbsent(readResult.transactionId(), key -> new HashSet()).add(readResult.rowId());
                    }
                }
            }
        }
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends ReadCommand> clo) -> {
            Command command = clo.command();

            assert false : "No read commands expected, [cmd=" + command + ']';
        });
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends WriteCommand> clo) -> {
            Command command = clo.command();

            long commandIndex = clo.index();
            long commandTerm = clo.term();

            // We choose the minimum applied index, since we choose it (the minimum one) on local recovery so as not to lose the data for
            // one of the storages.
            long storagesAppliedIndex = Math.min(storage.lastAppliedIndex(), txStateStorage.lastAppliedIndex());

            assert commandIndex > storagesAppliedIndex :
                    "Write command must have an index greater than that of storages [commandIndex=" + commandIndex
                            + ", mvAppliedIndex=" + storage.lastAppliedIndex()
                            + ", txStateAppliedIndex=" + txStateStorage.lastAppliedIndex() + "]";

            // NB: Make sure that ANY command we accept here updates lastAppliedIndex+term info in one of the underlying
            // storages!
            // Otherwise, a gap between lastAppliedIndex from the point of view of JRaft and our storage might appear.
            // If a leader has such a gap, and does doSnapshot(), it will subsequently truncate its log too aggressively
            // in comparison with 'snapshot' state stored in our storages; and if we install a snapshot from our storages
            // to a follower at this point, for a subsequent AppendEntries the leader will not be able to get prevLogTerm
            // (because it's already truncated in the leader's log), so it will have to install a snapshot again, and then
            // repeat same thing over and over again.

            storage.acquirePartitionSnapshotsReadLock();

            try {
                if (command instanceof UpdateCommand) {
                    handleUpdateCommand((UpdateCommand) command, commandIndex, commandTerm);
                } else if (command instanceof UpdateAllCommand) {
                    handleUpdateAllCommand((UpdateAllCommand) command, commandIndex, commandTerm);
                } else if (command instanceof FinishTxCommand) {
                    handleFinishTxCommand((FinishTxCommand) command, commandIndex, commandTerm);
                } else if (command instanceof TxCleanupCommand) {
                    handleTxCleanupCommand((TxCleanupCommand) command, commandIndex, commandTerm);
                } else if (command instanceof SafeTimeSyncCommand) {
                    handleSafeTimeSyncCommand((SafeTimeSyncCommand) command, commandIndex, commandTerm);
                } else {
                    assert false : "Command was not found [cmd=" + command + ']';
                }

                clo.result(null);
            } catch (IgniteInternalException e) {
                clo.result(e);
            } finally {
                storage.releasePartitionSnapshotsReadLock();
            }

            if (command instanceof SafeTimePropagatingCommand) {
                SafeTimePropagatingCommand safeTimePropagatingCommand = (SafeTimePropagatingCommand) command;

                assert safeTimePropagatingCommand.safeTime() != null;

                safeTime.update(safeTimePropagatingCommand.safeTime().asHybridTimestamp());
            }

            storageIndexTracker.update(commandIndex);
        });
    }

    /**
     * Handler for the {@link UpdateCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     */
    private void handleUpdateCommand(UpdateCommand cmd, long commandIndex, long commandTerm) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return;
        }

        storageUpdateHandler.handleUpdate(cmd.txId(), cmd.rowUuid(), cmd.tablePartitionId().asTablePartitionId(), cmd.rowBuffer(),
                rowId -> {
                    txsPendingRowIds.computeIfAbsent(cmd.txId(), entry -> new HashSet<>()).add(rowId);

                    storage.lastApplied(commandIndex, commandTerm);
                }
        );
    }

    /**
     * Handler for the {@link UpdateAllCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     */
    private void handleUpdateAllCommand(UpdateAllCommand cmd, long commandIndex, long commandTerm) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return;
        }

        storageUpdateHandler.handleUpdateAll(cmd.txId(), cmd.rowsToUpdate(), cmd.tablePartitionId().asTablePartitionId(), rowIds -> {
            for (RowId rowId : rowIds) {
                txsPendingRowIds.computeIfAbsent(cmd.txId(), entry0 -> new HashSet<>()).add(rowId);
            }

            storage.lastApplied(commandIndex, commandTerm);
        });
    }

    /**
     * Handler for the {@link FinishTxCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @throws IgniteInternalException if an exception occurred during a transaction state change.
     */
    private void handleFinishTxCommand(FinishTxCommand cmd, long commandIndex, long commandTerm) throws IgniteInternalException {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= txStateStorage.lastAppliedIndex()) {
            return;
        }

        UUID txId = cmd.txId();

        TxState stateToSet = cmd.commit() ? COMMITED : ABORTED;

        TxMeta txMetaToSet = new TxMeta(
                stateToSet,
                cmd.tablePartitionIds()
                        .stream()
                        .map(TablePartitionIdMessage::asTablePartitionId)
                        .collect(Collectors.toList()),
                cmd.commitTimestamp() != null ? cmd.commitTimestamp().asHybridTimestamp() : null
        );

        TxMeta txMetaBeforeCas = txStateStorage.get(txId);

        boolean txStateChangeRes = txStateStorage.compareAndSet(
                txId,
                null,
                txMetaToSet,
                commandIndex,
                commandTerm
        );

        LOG.debug("Finish the transaction txId = {}, state = {}, txStateChangeRes = {}", txId, txMetaToSet, txStateChangeRes);

        if (!txStateChangeRes) {
            UUID traceId = UUID.randomUUID();

            String errorMsg = format("Fail to finish the transaction txId = {} because of inconsistent state = {},"
                            + " expected state = null, state to set = {}",
                    txId,
                    txMetaBeforeCas,
                    txMetaToSet
            );

            IgniteInternalException stateChangeException = new IgniteInternalException(traceId, TX_UNEXPECTED_STATE_ERR, errorMsg);

            // Exception is explicitly logged because otherwise it can be lost if it did not occur on the leader.
            LOG.error(errorMsg);

            throw stateChangeException;
        }
    }


    /**
     * Handler for the {@link TxCleanupCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     */
    private void handleTxCleanupCommand(TxCleanupCommand cmd, long commandIndex, long commandTerm) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return;
        }

        UUID txId = cmd.txId();

        Set<RowId> pendingRowIds = txsPendingRowIds.getOrDefault(txId, Collections.emptySet());

        if (cmd.commit()) {
            storage.runConsistently(() -> {
                pendingRowIds.forEach(rowId -> storage.commitWrite(rowId, cmd.commitTimestamp().asHybridTimestamp()));

                txsPendingRowIds.remove(txId);

                storage.lastApplied(commandIndex, commandTerm);

                return null;
            });
        } else {
            storageUpdateHandler.handleTransactionAbortion(pendingRowIds, () -> {
                // on replication callback
                txsPendingRowIds.remove(txId);

                storage.lastApplied(commandIndex, commandTerm);
            });
        }
    }

    /**
     * Handler for the {@link SafeTimeSyncCommand}.
     *
     * @param cmd Command.
     * @param commandIndex RAFT index of the command.
     * @param commandTerm  RAFT term of the command.
     */
    private void handleSafeTimeSyncCommand(SafeTimeSyncCommand cmd, long commandIndex, long commandTerm) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return;
        }

        // We MUST bump information about last updated index+term.
        // See a comment in #onWrite() for explanation.
        storage.runConsistently(() -> {
            storage.lastApplied(commandIndex, commandTerm);

            return null;
        });
    }

    @Override
    public void onConfigurationCommitted(CommittedConfiguration config) {
        // Skips the update because the storage has already recorded it.
        if (config.index() <= storage.lastAppliedIndex()) {
            return;
        }

        // Do the update under lock to make sure no snapshot is started concurrently with this update.
        // Note that we do not need to protect from a concurrent command execution by this listener because
        // configuration is committed in the same thread in which commands are applied.
        storage.acquirePartitionSnapshotsReadLock();

        try {
            storage.runConsistently(() -> {
                storage.committedGroupConfiguration(
                        new RaftGroupConfiguration(config.peers(), config.learners(), config.oldPeers(), config.oldLearners())
                );
                storage.lastApplied(config.index(), config.term());

                return null;
            });
        } finally {
            storage.releasePartitionSnapshotsReadLock();
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        // The max index here is required for local recovery and a possible scenario
        // of false node failure when we actually have all required data. This might happen because we use the minimal index
        // among storages on a node restart.
        // Let's consider a more detailed example:
        //      1) We don't propagate the maximal lastAppliedIndex among storages, and onSnapshotSave finishes, it leads to the raft log
        //         truncation until the maximal lastAppliedIndex.
        //      2) Unexpected cluster restart happens.
        //      3) Local recovery of a node is started, where we request data from the minimal lastAppliedIndex among storages, because
        //         some data for some node might not have been flushed before unexpected cluster restart.
        //      4) When we try to restore data starting from the minimal lastAppliedIndex, we come to the situation
        //         that a raft node doesn't have such data, because the truncation until the maximal lastAppliedIndex from 1) has happened.
        //      5) Node cannot finish local recovery.
        long maxLastAppliedIndex = Math.max(storage.lastAppliedIndex(), txStateStorage.lastAppliedIndex());
        long maxLastAppliedTerm = Math.max(storage.lastAppliedTerm(), txStateStorage.lastAppliedTerm());

        storage.runConsistently(() -> {
            storage.lastApplied(maxLastAppliedIndex, maxLastAppliedTerm);

            return null;
        });

        txStateStorage.lastApplied(maxLastAppliedIndex, maxLastAppliedTerm);

        CompletableFuture.allOf(storage.flush(), txStateStorage.flush())
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        return true;
    }

    @Override
    public void onShutdown() {
        storage.close();
    }

    /**
     * Returns underlying storage.
     */
    @TestOnly
    public MvPartitionStorage getMvStorage() {
        return storage.getStorage();
    }
}
