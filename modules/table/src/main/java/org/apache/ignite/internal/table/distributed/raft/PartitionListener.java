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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.table.distributed.TableUtils.indexIdsAtRwTxBeginTs;
import static org.apache.ignite.internal.table.distributed.TableUtils.indexIdsAtRwTxBeginTsOrNull;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.util.CollectionUtils.last;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxBeginTimeCommand;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.raft.OnSnapshotSaveHandler;
import org.apache.ignite.internal.partition.replicator.raft.RaftTableProcessor;
import org.apache.ignite.internal.partition.replicator.raft.RaftTxFinishMarker;
import org.apache.ignite.internal.partition.replicator.raft.handlers.FinishTxCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowUpgrader;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.table.distributed.raft.handlers.MinimumActiveTxTimeCommandHandler;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.tx.message.VacuumTxStatesCommand;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener, RaftTableProcessor {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionListener.class);

    /** Transaction manager. */
    private final TxManager txManager;

    /** Partition storage with access to MV data of a partition. */
    private final PartitionDataStorage storage;

    /** Handler that processes storage updates. */
    private final StorageUpdateHandler storageUpdateHandler;

    /** Storage of transaction metadata. */
    private final TxStatePartitionStorage txStatePartitionStorage;

    /** Safe time tracker. */
    private final SafeTimeValuesTracker safeTimeTracker;

    /** Storage index tracker. */
    private final PendingComparableValuesTracker<Long, Void> storageIndexTracker;

    private final CatalogService catalogService;

    private final SchemaRegistry schemaRegistry;

    private final IndexMetaStorage indexMetaStorage;

    private final UUID localNodeId;

    // This variable is volatile, because it may be updated outside the Raft thread under the colocation feature.
    private volatile Set<String> currentGroupTopology;

    private final OnSnapshotSaveHandler onSnapshotSaveHandler;

    // Raft command handlers.
    private final RaftTxFinishMarker txFinisher;

    private final FinishTxCommandHandler finishTxCommandHandler;

    private final MinimumActiveTxTimeCommandHandler minimumActiveTxTimeCommandHandler;

    /** Constructor. */
    public PartitionListener(
            TxManager txManager,
            PartitionDataStorage partitionDataStorage,
            StorageUpdateHandler storageUpdateHandler,
            TxStatePartitionStorage txStatePartitionStorage,
            SafeTimeValuesTracker safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            CatalogService catalogService,
            SchemaRegistry schemaRegistry,
            IndexMetaStorage indexMetaStorage,
            UUID localNodeId,
            MinimumRequiredTimeCollectorService minTimeCollectorService
    ) {
        this.txManager = txManager;
        this.storage = partitionDataStorage;
        this.storageUpdateHandler = storageUpdateHandler;
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.safeTimeTracker = safeTimeTracker;
        this.storageIndexTracker = storageIndexTracker;
        this.catalogService = catalogService;
        this.schemaRegistry = schemaRegistry;
        this.indexMetaStorage = indexMetaStorage;
        this.localNodeId = localNodeId;

        onSnapshotSaveHandler = new OnSnapshotSaveHandler(txStatePartitionStorage, storageIndexTracker);

        // RAFT command handlers initialization.
        TablePartitionId tablePartitionId = new TablePartitionId(storage.tableId(), storage.partitionId());
        txFinisher = new RaftTxFinishMarker(txManager);
        finishTxCommandHandler = new FinishTxCommandHandler(
                txStatePartitionStorage,
                tablePartitionId,
                txManager);

        minimumActiveTxTimeCommandHandler = new MinimumActiveTxTimeCommandHandler(
                storage,
                tablePartitionId,
                minTimeCollectorService);
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
            WriteCommand command = clo.command();

            long commandIndex = clo.index();
            long commandTerm = clo.term();
            @Nullable HybridTimestamp safeTimestamp = clo.safeTimestamp();
            assert safeTimestamp == null || command instanceof SafeTimePropagatingCommand : command;

            // We choose the minimum applied index, since we choose it (the minimum one) on local recovery so as not to lose the data for
            // one of the storages.
            long storagesAppliedIndex = Math.min(storage.lastAppliedIndex(), txStatePartitionStorage.lastAppliedIndex());

            assert commandIndex > storagesAppliedIndex :
                    "Write command must have an index greater than that of storages [commandIndex=" + commandIndex
                            + ", mvAppliedIndex=" + storage.lastAppliedIndex()
                            + ", txStateAppliedIndex=" + txStatePartitionStorage.lastAppliedIndex() + "]";

            IgniteBiTuple<Serializable, Boolean> result = null;

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
                result = processCommand(command, commandIndex, commandTerm, safeTimestamp);
            } catch (Throwable t) {
                LOG.error(
                        "Got error while processing command [commandIndex={}, commandTerm={}, command={}]",
                        t,
                        clo.index(), clo.index(), command
                );

                clo.result(t);

                throw t;
            } finally {
                storage.releasePartitionSnapshotsReadLock();
            }

            // Completing the closure out of the partition snapshots lock to reduce possibility of deadlocks as it might
            // trigger other actions taking same locks.
            clo.result(result.get1());
        });
    }

    @Override
    public IgniteBiTuple<Serializable, Boolean> processCommand(
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        IgniteBiTuple<Serializable, Boolean> result;

        if (command instanceof UpdateCommand) {
            result = handleUpdateCommand((UpdateCommand) command, commandIndex, commandTerm, safeTimestamp);
        } else if (command instanceof UpdateAllCommand) {
            result = handleUpdateAllCommand((UpdateAllCommand) command, commandIndex, commandTerm, safeTimestamp);
        } else if (command instanceof FinishTxCommand) {
            result = finishTxCommandHandler.handle((FinishTxCommand) command, commandIndex, commandTerm);
        } else if (command instanceof WriteIntentSwitchCommand) {
            result = handleWriteIntentSwitchCommand((WriteIntentSwitchCommand) command, commandIndex, commandTerm);
        } else if (command instanceof SafeTimeSyncCommand) {
            result = handleSafeTimeSyncCommand((SafeTimeSyncCommand) command, commandIndex, commandTerm);
        } else if (command instanceof BuildIndexCommand) {
            result = handleBuildIndexCommand((BuildIndexCommand) command, commandIndex, commandTerm);
        } else if (command instanceof PrimaryReplicaChangeCommand) {
            result = handlePrimaryReplicaChangeCommand((PrimaryReplicaChangeCommand) command, commandIndex, commandTerm);
        } else if (command instanceof VacuumTxStatesCommand) {
            result = handleVacuumTxStatesCommand((VacuumTxStatesCommand) command, commandIndex, commandTerm);
        } else if (command instanceof UpdateMinimumActiveTxBeginTimeCommand) {
            result = minimumActiveTxTimeCommandHandler.handle((UpdateMinimumActiveTxBeginTimeCommand) command, commandIndex);
        } else {
            throw new AssertionError("Unknown command type [command=" + command.toStringForLightLogging() + ']');
        }

        if (Boolean.TRUE.equals(result.get2())) {
            // Adjust safe time before completing update to reduce waiting.
            if (safeTimestamp != null) {
                updateTrackerIgnoringTrackerClosedException(safeTimeTracker, safeTimestamp);
            }

            updateTrackerIgnoringTrackerClosedException(storageIndexTracker, commandIndex);
        }

        return result;
    }

    /**
     * Handler for the {@link UpdateCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @param safeTimestamp Safe timestamp.
     * @return The result.
     */
    private IgniteBiTuple<Serializable, Boolean> handleUpdateCommand(
            UpdateCommand cmd,
            long commandIndex,
            long commandTerm,
            HybridTimestamp safeTimestamp
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false); // Update result is not needed.
        }

        if (cmd.leaseStartTime() != null) {
            long leaseStartTime = cmd.leaseStartTime();

            long storageLeaseStartTime = storage.leaseStartTime();

            if (leaseStartTime != storageLeaseStartTime) {
                return new IgniteBiTuple<>(
                        new UpdateCommandResult(false, storageLeaseStartTime, isPrimaryInGroupTopology(), NULL_HYBRID_TIMESTAMP), false);
            }
        }

        UUID txId = cmd.txId();

        assert storage.primaryReplicaNodeId() != null;
        assert localNodeId != null;

        if (cmd.full() || !localNodeId.equals(storage.primaryReplicaNodeId())) {
            storageUpdateHandler.handleUpdate(
                    txId,
                    cmd.rowUuid(),
                    cmd.commitPartitionId().asTablePartitionId(),
                    cmd.rowToUpdate(),
                    !cmd.full(),
                    () -> storage.lastApplied(commandIndex, commandTerm),
                    cmd.full() ? safeTimestamp : null,
                    cmd.lastCommitTimestamp(),
                    indexIdsAtRwTxBeginTs(catalogService, txId, storage.tableId())
            );
        } else {
            // We MUST bump information about last updated index+term.
            // See a comment in #onWrite() for explanation.
            // If we get here, that means that we are collocated with primary and data was already inserted there, thus it's only required
            // to update information about index and term.
            advanceLastAppliedIndexConsistently(commandIndex, commandTerm);
        }

        replicaTouch(txId, cmd.txCoordinatorId(), cmd.full() ? safeTimestamp : null, cmd.full());

        return new IgniteBiTuple<>(new UpdateCommandResult(true, isPrimaryInGroupTopology(), safeTimestamp.longValue()), true);
    }

    /**
     * Handler for the {@link UpdateAllCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @param safeTimestamp Safe timestamp.
     */
    private IgniteBiTuple<Serializable, Boolean> handleUpdateAllCommand(
            UpdateAllCommand cmd,
            long commandIndex,
            long commandTerm,
            HybridTimestamp safeTimestamp
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        if (cmd.leaseStartTime() != null) {
            long leaseStartTime = cmd.leaseStartTime();

            long storageLeaseStartTime = storage.leaseStartTime();

            if (leaseStartTime != storageLeaseStartTime) {
                return new IgniteBiTuple<>(
                        new UpdateCommandResult(false, storageLeaseStartTime, isPrimaryInGroupTopology(), NULL_HYBRID_TIMESTAMP), false);
            }
        }

        UUID txId = cmd.txId();

        if (cmd.full() || !localNodeId.equals(storage.primaryReplicaNodeId())) {
            storageUpdateHandler.handleUpdateAll(
                    txId,
                    cmd.rowsToUpdate(),
                    cmd.commitPartitionId().asTablePartitionId(),
                    !cmd.full(),
                    () -> storage.lastApplied(commandIndex, commandTerm),
                    cmd.full() ? safeTimestamp : null,
                    indexIdsAtRwTxBeginTs(catalogService, txId, storage.tableId())
            );
        } else {
            // We MUST bump information about last updated index+term.
            // See a comment in #onWrite() for explanation.
            // If we get here, that means that we are collocated with primary and data was already inserted there, thus it's only required
            // to update information about index and term.
            advanceLastAppliedIndexConsistently(commandIndex, commandTerm);
        }

        replicaTouch(txId, cmd.txCoordinatorId(), cmd.full() ? safeTimestamp : null, cmd.full());

        return new IgniteBiTuple<>(new UpdateCommandResult(true, isPrimaryInGroupTopology(), safeTimestamp.longValue()), true);
    }

    /**
     * Handler for the {@link WriteIntentSwitchCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     */
    private IgniteBiTuple<Serializable, Boolean> handleWriteIntentSwitchCommand(
            WriteIntentSwitchCommand cmd,
            long commandIndex,
            long commandTerm
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        UUID txId = cmd.txId();

        txFinisher.markFinished(txId, cmd.commit(), cmd.commitTimestamp(), null);

        storageUpdateHandler.switchWriteIntents(
                txId,
                cmd.commit(),
                cmd.commitTimestamp(),
                () -> storage.lastApplied(commandIndex, commandTerm),
                indexIdsAtRwTxBeginTsOrNull(catalogService, txId, storage.tableId())
        );

        return new IgniteBiTuple<>(null, true);
    }

    /**
     * Handler for the {@link SafeTimeSyncCommand}.
     *
     * @param cmd Command.
     * @param commandIndex RAFT index of the command.
     * @param commandTerm RAFT term of the command.
     */
    private IgniteBiTuple<Serializable, Boolean> handleSafeTimeSyncCommand(SafeTimeSyncCommand cmd, long commandIndex, long commandTerm) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        // We MUST bump information about last updated index+term.
        // See a comment in #onWrite() for explanation.
        advanceLastAppliedIndexConsistently(commandIndex, commandTerm);

        return new IgniteBiTuple<>(null, true);
    }

    private void advanceLastAppliedIndexConsistently(long commandIndex, long commandTerm) {
        storage.runConsistently(locker -> {
            storage.lastApplied(commandIndex, commandTerm);

            return null;
        });
    }

    @Override
    public void onConfigurationCommitted(
            RaftGroupConfiguration config,
            long lastAppliedIndex,
            long lastAppliedTerm
    ) {
        currentGroupTopology = new HashSet<>(config.peers());
        currentGroupTopology.addAll(config.learners());

        // Skips the update because the storage has already recorded it.
        if (config.index() <= storage.lastAppliedIndex()) {
            return;
        }

        // Do the update under lock to make sure no snapshot is started concurrently with this update.
        // Note that we do not need to protect from a concurrent command execution by this listener because
        // configuration is committed in the same thread in which commands are applied.
        storage.acquirePartitionSnapshotsReadLock();

        try {
            storage.runConsistently(locker -> {
                storage.committedGroupConfiguration(config);
                storage.lastApplied(lastAppliedIndex, lastAppliedTerm);
                updateTrackerIgnoringTrackerClosedException(storageIndexTracker, config.index());

                return null;
            });
        } finally {
            storage.releasePartitionSnapshotsReadLock();
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        onSnapshotSaveHandler.onSnapshotSave(List.of(this))
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

    @Override
    public long lastAppliedIndex() {
        return storage.lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return storage.lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        storage.runConsistently(locker -> {
            storage.lastApplied(lastAppliedIndex, lastAppliedTerm);

            return null;
        });
    }

    @Override
    public CompletableFuture<Void> flushStorage() {
        return storage.flush();
    }

    /**
     * Returns underlying storage.
     */
    @TestOnly
    public MvPartitionStorage getMvStorage() {
        return storage.getStorage();
    }

    /**
     * Returns safe timestamp.
     */
    @TestOnly
    public PendingComparableValuesTracker<HybridTimestamp, Void> getSafeTimeTracker() {
        return safeTimeTracker;
    }

    /**
     * Handler for the {@link BuildIndexCommand}.
     *
     * @param cmd Command.
     * @param commandIndex RAFT index of the command.
     * @param commandTerm RAFT term of the command.
     */
    IgniteBiTuple<Serializable, Boolean> handleBuildIndexCommand(BuildIndexCommand cmd, long commandIndex, long commandTerm) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        IndexMeta indexMeta = indexMetaStorage.indexMeta(cmd.indexId());

        if (indexMeta == null || indexMeta.isDropped()) {
            // Index has been dropped.
            return new IgniteBiTuple<>(null, true);
        }

        BuildIndexRowVersionChooser rowVersionChooser = createBuildIndexRowVersionChooser(indexMeta);

        BinaryRowUpgrader binaryRowUpgrader = createBinaryRowUpgrader(indexMeta);

        storage.runConsistently(locker -> {
            List<UUID> rowUuids = new ArrayList<>(cmd.rowIds());

            // Natural UUID order matches RowId order within the same partition.
            Collections.sort(rowUuids);

            Stream<BinaryRowAndRowId> buildIndexRowStream = createBuildIndexRowStream(
                    rowUuids,
                    locker,
                    rowVersionChooser,
                    binaryRowUpgrader
            );

            RowId nextRowIdToBuild = cmd.finish() ? null : toRowId(requireNonNull(last(rowUuids))).increment();

            storageUpdateHandler.getIndexUpdateHandler().buildIndex(cmd.indexId(), buildIndexRowStream, nextRowIdToBuild);

            storage.lastApplied(commandIndex, commandTerm);

            return null;
        });

        if (cmd.finish()) {
            LOG.info(
                    "Finish building the index: [tableId={}, partitionId={}, indexId={}]",
                    storage.tableId(), storage.partitionId(), cmd.indexId()
            );
        }

        return new IgniteBiTuple<>(null, true);
    }

    /**
     * Handler for {@link PrimaryReplicaChangeCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     */
    private IgniteBiTuple<Serializable, Boolean> handlePrimaryReplicaChangeCommand(
            PrimaryReplicaChangeCommand cmd,
            long commandIndex,
            long commandTerm
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        storage.runConsistently(locker -> {
            storage.updateLease(cmd.leaseStartTime(), cmd.primaryReplicaNodeId(), cmd.primaryReplicaNodeName());

            storage.lastApplied(commandIndex, commandTerm);

            return null;
        });

        return new IgniteBiTuple<>(null, true);
    }

    /**
     * Handler for {@link VacuumTxStatesCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     */
    private IgniteBiTuple<Serializable, Boolean>  handleVacuumTxStatesCommand(
            VacuumTxStatesCommand cmd,
            long commandIndex,
            long commandTerm
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return new IgniteBiTuple<>(null, false);
        }

        txStatePartitionStorage.removeAll(cmd.txIds(), commandIndex, commandTerm);

        return new IgniteBiTuple<>(null, true);
    }

    private static <T extends Comparable<T>> void updateTrackerIgnoringTrackerClosedException(
            PendingComparableValuesTracker<T, Void> tracker,
            T newValue
    ) {
        try {
            tracker.update(newValue, null);
        } catch (TrackerClosedException ignored) {
            // No-op.
        }
    }

    private Stream<BinaryRowAndRowId> createBuildIndexRowStream(
            List<UUID> rowUuids,
            Locker locker,
            BuildIndexRowVersionChooser rowVersionChooser,
            BinaryRowUpgrader binaryRowUpgrader
    ) {
        return rowUuids.stream()
                .map(this::toRowId)
                .peek(locker::lock)
                .map(rowVersionChooser::chooseForBuildIndex)
                .flatMap(Collection::stream)
                .map(binaryRowAndRowId -> upgradeBinaryRow(binaryRowUpgrader, binaryRowAndRowId));
    }

    private RowId toRowId(UUID rowUuid) {
        return new RowId(storageUpdateHandler.partitionId(), rowUuid);
    }

    private void replicaTouch(UUID txId, UUID txCoordinatorId, HybridTimestamp commitTimestamp, boolean full) {
        txManager.updateTxMeta(txId, old -> new TxStateMeta(
                full ? COMMITTED : PENDING,
                txCoordinatorId,
                old == null ? null : old.commitPartitionId(),
                full ? commitTimestamp : null,
                old == null ? null : old.tx(),
                old == null ? null : old.isFinishedDueToTimeout()
        ));
    }

    private BuildIndexRowVersionChooser createBuildIndexRowVersionChooser(IndexMeta indexMeta) {
        MetaIndexStatusChange registeredChangeInfo = indexMeta.statusChange(REGISTERED);
        MetaIndexStatusChange buildingChangeInfo = indexMeta.statusChange(BUILDING);

        return new BuildIndexRowVersionChooser(
                storage,
                registeredChangeInfo.activationTimestamp(),
                buildingChangeInfo.activationTimestamp()
        );
    }

    private BinaryRowUpgrader createBinaryRowUpgrader(IndexMeta indexMeta) {
        SchemaDescriptor schema = schemaRegistry.schema(indexMeta.tableVersion());

        return new BinaryRowUpgrader(schemaRegistry, schema);
    }

    private static BinaryRowAndRowId upgradeBinaryRow(BinaryRowUpgrader upgrader, BinaryRowAndRowId source) {
        BinaryRow sourceBinaryRow = source.binaryRow();
        BinaryRow upgradedBinaryRow = upgrader.upgrade(sourceBinaryRow);

        return upgradedBinaryRow == sourceBinaryRow ? source : new BinaryRowAndRowId(upgradedBinaryRow, source.rowId());
    }

    /**
     * Checks whether the primary replica belongs to the raft group topology (peers and learners) within a raft linearized context.
     * On the primary replica election prior to the lease publication, the placement driver sends a PrimaryReplicaChangeCommand that
     * populates the raft listener and the underneath storage with lease-related information, such as primaryReplicaNodeId,
     * primaryReplicaNodeName and leaseStartTime. In Update(All)Command  handling, which occurs strictly after PrimaryReplicaChangeCommand
     * processing, given information is used in order to detect whether primary belongs to the raft group topology (peers and learners).
     *
     *
     * @return {@code true} if primary replica belongs to the raft group topology: peers and learners, (@code false) otherwise.
     */
    private boolean isPrimaryInGroupTopology() {
        assert currentGroupTopology != null : "Current group topology is null";

        if (storage.primaryReplicaNodeName() == null) {
            return true;
        } else {
            // Despite the fact that storage.primaryReplicaNodeName() may itself return null it's never expected to happen
            // while calling isPrimaryInGroupTopology because of HB between handlePrimaryReplicaChangeCommand that will populate the storage
            // with lease information and handleUpdate(All)Command that on it's turn calls isPrimaryReplicaInGroupTopology.
            assert storage.primaryReplicaNodeName() != null : "Primary replica node name is null.";
            return currentGroupTopology.contains(storage.primaryReplicaNodeName());
        }
    }
}
