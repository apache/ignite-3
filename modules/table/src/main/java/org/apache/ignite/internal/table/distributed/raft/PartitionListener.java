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

import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.Commands.BUILD_INDEX_V1;
import static org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.Commands.BUILD_INDEX_V2;
import static org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.Commands.BUILD_INDEX_V3;
import static org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.Commands.UPDATE_MINIMUM_ACTIVE_TX_TIME_COMMAND;
import static org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.GROUP_TYPE;
import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_APPLIED_RESULT;
import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_NOT_APPLIED_RESULT;
import static org.apache.ignite.internal.table.distributed.TableUtils.indexIdsAtRwTxBeginTs;
import static org.apache.ignite.internal.table.distributed.TableUtils.indexIdsAtRwTxBeginTsOrNull;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.PENDING;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.raft.CommandResult;
import org.apache.ignite.internal.partition.replicator.raft.RaftTableProcessor;
import org.apache.ignite.internal.partition.replicator.raft.handlers.AbstractCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.handlers.CommandHandlers;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.handlers.BuildIndexCommandHandler;
import org.apache.ignite.internal.table.distributed.raft.handlers.MinimumActiveTxTimeCommandHandler;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
// TODO ignite-22522 Rename to TablePartitionProcessor and remove implements RaftGroupListener
public class PartitionListener implements RaftTableProcessor {
    /** Transaction manager. */
    private final TxManager txManager;

    /** Partition storage with access to MV data of a partition. */
    private final PartitionDataStorage storage;

    /** Handler that processes storage updates. */
    private final StorageUpdateHandler storageUpdateHandler;

    /** Safe time tracker. */
    private final SafeTimeValuesTracker safeTimeTracker;

    private final CatalogService catalogService;

    private final UUID localNodeId;

    private final Set<String> currentGroupTopology = new HashSet<>();

    // Raft command handlers.
    private final CommandHandlers commandHandlers;

    private final LeasePlacementDriver placementDriver;

    private final ClockService clockService;

    /**
     * Partition group ID that is actually used for replication.
     */
    private final ZonePartitionId realReplicationGroupId;

    private ReplicaMeta lastKnownLease;

    /** Constructor. */
    public PartitionListener(
            TxManager txManager,
            PartitionDataStorage partitionDataStorage,
            StorageUpdateHandler storageUpdateHandler,
            SafeTimeValuesTracker safeTimeTracker,
            CatalogService catalogService,
            SchemaRegistry schemaRegistry,
            IndexMetaStorage indexMetaStorage,
            UUID localNodeId,
            MinimumRequiredTimeCollectorService minTimeCollectorService,
            Executor partitionOperationsExecutor,
            LeasePlacementDriver placementDriver,
            ClockService clockService,
            ZonePartitionId realReplicationGroupId
    ) {
        this.txManager = txManager;
        this.storage = partitionDataStorage;
        this.storageUpdateHandler = storageUpdateHandler;
        this.safeTimeTracker = safeTimeTracker;
        this.catalogService = catalogService;
        this.localNodeId = localNodeId;
        this.placementDriver = placementDriver;
        this.clockService = clockService;
        this.realReplicationGroupId = realReplicationGroupId;

        // RAFT command handlers initialization.
        TablePartitionId tablePartitionId = new TablePartitionId(storage.tableId(), storage.partitionId());

        CommandHandlers.Builder commandHandlersBuilder = new CommandHandlers.Builder();
        commandHandlersBuilder.addHandler(GROUP_TYPE, UPDATE_MINIMUM_ACTIVE_TX_TIME_COMMAND, new MinimumActiveTxTimeCommandHandler(
                storage,
                tablePartitionId,
                minTimeCollectorService
        ));

        BuildIndexCommandHandler buildIndexCommandHandler = new BuildIndexCommandHandler(
                storage,
                indexMetaStorage,
                storageUpdateHandler,
                schemaRegistry
        );
        commandHandlersBuilder.addHandler(GROUP_TYPE, BUILD_INDEX_V1, buildIndexCommandHandler);
        commandHandlersBuilder.addHandler(GROUP_TYPE, BUILD_INDEX_V2, buildIndexCommandHandler);
        commandHandlersBuilder.addHandler(GROUP_TYPE, BUILD_INDEX_V3, buildIndexCommandHandler);

        this.commandHandlers = commandHandlersBuilder.build();

        RaftGroupConfiguration committedGroupConfiguration = storage.committedGroupConfiguration();

        if (committedGroupConfiguration != null) {
            setCurrentGroupTopology(committedGroupConfiguration);
        }
    }

    private boolean shouldUpdateStorage(boolean isFull, LeaseInfo storageLeaseInfo) {
        if (isFull) {
            return true;
        }

        HybridTimestamp currentTime = clockService.current();

        if (lastKnownLease == null || lastKnownLease.getExpirationTime().compareTo(currentTime) < 0) {
            lastKnownLease = placementDriver.getCurrentPrimaryReplica(realReplicationGroupId, currentTime);
        }

        if (lastKnownLease == null || !lastKnownLease.getLeaseholderId().equals(localNodeId)) {
            return true;
        } else {
            return !localNodeId.equals(storageLeaseInfo.primaryReplicaNodeId());
        }
    }
//
//    @Override
//    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
//        iterator.forEachRemaining((CommandClosure<? extends WriteCommand> clo) -> {
//            WriteCommand command = clo.command();
//
//            long commandIndex = clo.index();
//            long commandTerm = clo.term();
//            @Nullable HybridTimestamp safeTimestamp = clo.safeTimestamp();
//            assert safeTimestamp == null || command instanceof SafeTimePropagatingCommand : command;
//
//            long storagesAppliedIndex = storage.lastAppliedIndex();
//
//            assert commandIndex > storagesAppliedIndex :
//                    "Write command must have an index greater than that of storages [commandIndex=" + commandIndex
//                            + ", mvAppliedIndex=" + storage.lastAppliedIndex() + "]";
//
//            CommandResult result;
//
//            // NB: Make sure that ANY command we accept here updates lastAppliedIndex+term info in one of the underlying
//            // storages!
//            // Otherwise, a gap between lastAppliedIndex from the point of view of JRaft and our storage might appear.
//            // If a leader has such a gap, and does doSnapshot(), it will subsequently truncate its log too aggressively
//            // in comparison with 'snapshot' state stored in our storages; and if we install a snapshot from our storages
//            // to a follower at this point, for a subsequent AppendEntries the leader will not be able to get prevLogTerm
//            // (because it's already truncated in the leader's log), so it will have to install a snapshot again, and then
//            // repeat same thing over and over again.
//
//            storage.acquirePartitionSnapshotsReadLock();
//
//            try {
//                result = processCommand(command, commandIndex, commandTerm, safeTimestamp);
//            } catch (Throwable t) {
//                LOG.error(
//                        "Got error while processing command [commandIndex={}, commandTerm={}, command={}]",
//                        t,
//                        clo.index(), clo.index(), command
//                );
//
//                clo.result(t);
//
//                throw t;
//            } finally {
//                storage.releasePartitionSnapshotsReadLock();
//            }
//
//            // Completing the closure out of the partition snapshots lock to reduce possibility of deadlocks as it might
//            // trigger other actions taking same locks.
//            clo.result(result.result());
//        });
//    }

    @Override
    public CommandResult processCommand(
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        CommandResult result = null;

        AbstractCommandHandler<?> commandHandler = commandHandlers.handler(command.groupType(), command.messageType());

        if (commandHandler != null) {
            result = commandHandler.handle(command, commandIndex, commandTerm, safeTimestamp);
        } else if (command instanceof UpdateCommand) {
            result = handleUpdateCommand((UpdateCommand) command, commandIndex, commandTerm, safeTimestamp);
        } else if (command instanceof UpdateAllCommand) {
            result = handleUpdateAllCommand((UpdateAllCommand) command, commandIndex, commandTerm, safeTimestamp);
        } else if (command instanceof WriteIntentSwitchCommand) {
            result = handleWriteIntentSwitchCommand((WriteIntentSwitchCommand) command, commandIndex, commandTerm);
        } else if (command instanceof SafeTimeSyncCommand) {
            result = handleSafeTimeSyncCommand((SafeTimeSyncCommand) command, commandIndex, commandTerm);
        } else if (command instanceof PrimaryReplicaChangeCommand) {
            result = handlePrimaryReplicaChangeCommand((PrimaryReplicaChangeCommand) command, commandIndex, commandTerm);
        }

        if (result == null) {
            throw new AssertionError("Unknown command type [command=" + command.toStringForLightLogging() + ']');
        }

        if (result.wasApplied()) {
            // Adjust safe time before completing update to reduce waiting.
            if (safeTimestamp != null) {
                updateTrackerIgnoringTrackerClosedException(safeTimeTracker, safeTimestamp);
            }
        }

        return result;
    }

    @Override
    public void initialize(
            @Nullable RaftGroupConfiguration config,
            @Nullable LeaseInfo leaseInfo,
            long lastAppliedIndex,
            long lastAppliedTerm
    ) {
        assert storage.lastAppliedIndex() == 0 || storage.lastAppliedIndex() >= lastAppliedIndex : String.format(
                "Trying to initialize a non-empty storage with data with a greater applied index: "
                        + "storageLastAppliedIndex=%d, lastAppliedIndex=%d",
                storage.lastAppliedIndex(),
                lastAppliedIndex
        );

        if (lastAppliedIndex <= storage.lastAppliedIndex()) {
            return;
        }

        storage.runConsistently(locker -> {
            if (config != null) {
                setCurrentGroupTopology(config);

                storage.committedGroupConfiguration(config);
            }

            if (leaseInfo != null) {
                storage.updateLease(leaseInfo);
            }

            storage.lastApplied(lastAppliedIndex, lastAppliedTerm);

            return null;
        });

        // Initiate a flush but do not wait for it. This is needed to save the initialization information as soon as possible (to make
        // recovery more efficient), without blocking the caller thread.
        storage.flush();
    }

    /**
     * Handler for the {@link UpdateCommand}.
     *
     * <p>We will also handle {@link UpdateCommandV2}, since there is no specific logic for {@link UpdateCommandV2}, we will leave it as is
     * to support backward compatibility.</p>
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @param safeTimestamp Safe timestamp.
     * @return The result.
     */
    private CommandResult handleUpdateCommand(
            UpdateCommand cmd,
            long commandIndex,
            long commandTerm,
            HybridTimestamp safeTimestamp
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return EMPTY_NOT_APPLIED_RESULT; // Update result is not needed.
        }

        LeaseInfo storageLeaseInfo = storage.leaseInfo();

        if (cmd.leaseStartTime() != null) {
            long leaseStartTime = cmd.leaseStartTime();

            if (storageLeaseInfo == null || leaseStartTime != storageLeaseInfo.leaseStartTime()) {
                var updateCommandResult = new UpdateCommandResult(
                        false,
                        storageLeaseInfo == null ? 0 : storageLeaseInfo.leaseStartTime(),
                        isPrimaryInGroupTopology(storageLeaseInfo),
                        NULL_HYBRID_TIMESTAMP
                );

                return new CommandResult(updateCommandResult, false);
            }
        }

        UUID txId = cmd.txId();

        assert storageLeaseInfo != null;
        assert localNodeId != null;

        if (shouldUpdateStorage(cmd.full(), storageLeaseInfo)) {
            storageUpdateHandler.handleUpdate(
                    txId,
                    cmd.rowUuid(),
                    cmd.commitPartitionId().asReplicationGroupId(),
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

        return new CommandResult(
                new UpdateCommandResult(true, isPrimaryInGroupTopology(storageLeaseInfo), safeTimestamp.longValue()),
                true
        );
    }

    /**
     * Handler for the {@link UpdateAllCommand}.
     *
     * <p>We will also handle {@link UpdateAllCommandV2}, since there is no specific logic for {@link UpdateAllCommandV2}, we will leave it
     * as is to support backward compatibility.</p>
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @param safeTimestamp Safe timestamp.
     */
    private CommandResult handleUpdateAllCommand(
            UpdateAllCommand cmd,
            long commandIndex,
            long commandTerm,
            HybridTimestamp safeTimestamp
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return EMPTY_NOT_APPLIED_RESULT;
        }

        LeaseInfo storageLeaseInfo = storage.leaseInfo();

        if (cmd.leaseStartTime() != null) {
            long leaseStartTime = cmd.leaseStartTime();

            if (storageLeaseInfo == null || leaseStartTime != storageLeaseInfo.leaseStartTime()) {
                var updateCommandResult = new UpdateCommandResult(
                        false,
                        storageLeaseInfo == null ? 0 : storageLeaseInfo.leaseStartTime(),
                        isPrimaryInGroupTopology(storageLeaseInfo),
                        NULL_HYBRID_TIMESTAMP
                );

                return new CommandResult(updateCommandResult, false);
            }
        }

        UUID txId = cmd.txId();

        if (shouldUpdateStorage(cmd.full(), storageLeaseInfo)) {
            storageUpdateHandler.handleUpdateAll(
                    txId,
                    cmd.rowsToUpdate(),
                    cmd.commitPartitionId().asReplicationGroupId(),
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

        return new CommandResult(
                new UpdateCommandResult(true, isPrimaryInGroupTopology(storageLeaseInfo), safeTimestamp.longValue()),
                true
        );
    }

    /**
     * Handler for the {@link WriteIntentSwitchCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     */
    private CommandResult handleWriteIntentSwitchCommand(
            WriteIntentSwitchCommand cmd,
            long commandIndex,
            long commandTerm
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return EMPTY_NOT_APPLIED_RESULT;
        }

        UUID txId = cmd.txId();

        storageUpdateHandler.switchWriteIntents(
                txId,
                cmd.commit(),
                cmd.commitTimestamp(),
                () -> storage.lastApplied(commandIndex, commandTerm),
                indexIdsAtRwTxBeginTsOrNull(catalogService, txId, storage.tableId())
        );

        return EMPTY_APPLIED_RESULT;
    }

    /**
     * Handler for the {@link SafeTimeSyncCommand}.
     *
     * @param cmd Command.
     * @param commandIndex RAFT index of the command.
     * @param commandTerm RAFT term of the command.
     */
    private CommandResult handleSafeTimeSyncCommand(SafeTimeSyncCommand cmd, long commandIndex, long commandTerm) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return EMPTY_NOT_APPLIED_RESULT;
        }

        // We MUST bump information about last updated index+term.
        // See a comment in #onWrite() for explanation.
        advanceLastAppliedIndexConsistently(commandIndex, commandTerm);

        return EMPTY_APPLIED_RESULT;
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
        // Skips the update because the storage has already recorded it.
        if (config.index() <= storage.lastAppliedIndex()) {
            return;
        }

        setCurrentGroupTopology(config);

        // Do the update under lock to make sure no snapshot is started concurrently with this update.
        // Note that we do not need to protect from a concurrent command execution by this listener because
        // configuration is committed in the same thread in which commands are applied.
        storage.acquirePartitionSnapshotsReadLock();

        try {
            storage.runConsistently(locker -> {
                storage.committedGroupConfiguration(config);
                storage.lastApplied(lastAppliedIndex, lastAppliedTerm);

                return null;
            });
        } finally {
            storage.releasePartitionSnapshotsReadLock();
        }
    }

    private void setCurrentGroupTopology(RaftGroupConfiguration config) {
        currentGroupTopology.clear();
        currentGroupTopology.addAll(config.peers());
        currentGroupTopology.addAll(config.learners());
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
        if (lastAppliedIndex <= storage.lastAppliedIndex()) {
            return;
        }

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
     * Handler for {@link PrimaryReplicaChangeCommand}.
     *
     * @param cmd Command.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     */
    private CommandResult handlePrimaryReplicaChangeCommand(
            PrimaryReplicaChangeCommand cmd,
            long commandIndex,
            long commandTerm
    ) {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return EMPTY_NOT_APPLIED_RESULT;
        }

        storage.runConsistently(locker -> {
            var leaseInfo = new LeaseInfo(cmd.leaseStartTime(), cmd.primaryReplicaNodeId(), cmd.primaryReplicaNodeName());

            storage.updateLease(leaseInfo);

            storage.lastApplied(commandIndex, commandTerm);

            return null;
        });

        return EMPTY_APPLIED_RESULT;
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

    private void replicaTouch(UUID txId, UUID txCoordinatorId, HybridTimestamp commitTimestamp, boolean full) {
        txManager.updateTxMeta(txId, old -> TxStateMeta.builder(old, full ? COMMITTED : PENDING)
                .txCoordinatorId(txCoordinatorId)
                .commitTimestamp(full ? commitTimestamp : null)
                .build()
        );
    }

    /**
     * Checks whether the primary replica belongs to the raft group topology (peers and learners) within a raft linearized context. On the
     * primary replica election prior to the lease publication, the placement driver sends a PrimaryReplicaChangeCommand that populates the
     * raft listener and the underneath storage with lease-related information, such as primaryReplicaNodeId, primaryReplicaNodeName and
     * leaseStartTime. In Update(All)Command  handling, which occurs strictly after PrimaryReplicaChangeCommand processing, given
     * information is used in order to detect whether primary belongs to the raft group topology (peers and learners).
     *
     * @return {@code true} if primary replica belongs to the raft group topology: peers and learners, (@code false) otherwise.
     */
    private boolean isPrimaryInGroupTopology(@Nullable LeaseInfo storageLeaseInfo) {
        // Despite the fact that storage.leaseInfo() may itself return null it's never expected to happen
        // while calling isPrimaryInGroupTopology because of HB between handlePrimaryReplicaChangeCommand that will populate the storage
        // with lease information and handleUpdate(All)Command that on it's turn calls isPrimaryReplicaInGroupTopology.
        return storageLeaseInfo == null || currentGroupTopology.contains(storageLeaseInfo.primaryReplicaNodeName());
    }
}
