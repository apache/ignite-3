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
import static org.apache.ignite.internal.tx.TxState.PENDING;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class TablePartitionProcessor implements RaftTableProcessor {
    private static final IgniteLogger LOG = Loggers.forClass(TablePartitionProcessor.class);

    /** Transaction manager. */
    private final TxManager txManager;

    /** Partition storage with access to MV data of a partition. */
    private final PartitionDataStorage storage;

    /** Handler that processes storage updates. */
    private final StorageUpdateHandler storageUpdateHandler;

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
    public TablePartitionProcessor(
            TxManager txManager,
            PartitionDataStorage partitionDataStorage,
            StorageUpdateHandler storageUpdateHandler,
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
        } else if (command instanceof PrimaryReplicaChangeCommand) {
            result = handlePrimaryReplicaChangeCommand((PrimaryReplicaChangeCommand) command, commandIndex, commandTerm);
        }

        if (result == null) {
            throw new AssertionError("Unknown command type [command=" + command.toStringForLightLogging() + ']');
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

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(config);
            storage.lastApplied(lastAppliedIndex, lastAppliedTerm);

            return null;
        });
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
        long storageLastAppliedIndex = storage.lastAppliedIndex();
        LOG.debug("Handling PrimaryReplicaChangeCommand [tableId={}, partId={}, commandIndex={}, storageLastAppliedIndex={}, "
                        + "leaseStartTime={}, primaryNodeId={}, primaryNodeName={}]",
                storage.tableId(), storage.partitionId(), commandIndex, storageLastAppliedIndex,
                cmd.leaseStartTime(), cmd.primaryReplicaNodeId(), cmd.primaryReplicaNodeName());

        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storageLastAppliedIndex) {
            LOG.debug("Skipping PrimaryReplicaChangeCommand - already applied [tableId={}, partId={}, commandIndex={}, "
                            + "storageLastAppliedIndex={}]",
                    storage.tableId(), storage.partitionId(), commandIndex, storageLastAppliedIndex);
            return EMPTY_NOT_APPLIED_RESULT;
        }

        storage.runConsistently(locker -> {
            var leaseInfo = new LeaseInfo(cmd.leaseStartTime(), cmd.primaryReplicaNodeId(), cmd.primaryReplicaNodeName());

            storage.updateLease(leaseInfo);

            storage.lastApplied(commandIndex, commandTerm);

            return null;
        });

        LOG.debug("Successfully applied PrimaryReplicaChangeCommand [tableId={}, partId={}, commandIndex={}, leaseStartTime={}]",
                storage.tableId(), storage.partitionId(), commandIndex, cmd.leaseStartTime());

        return EMPTY_APPLIED_RESULT;
    }

    private void replicaTouch(UUID txId, UUID txCoordinatorId, HybridTimestamp commitTimestamp, boolean full) {
        // Saving state is not needed for full transactions.
        if (!full) {
            txManager.updateTxMeta(txId, old -> TxStateMeta.builder(old, PENDING)
                    .txCoordinatorId(txCoordinatorId)
                    .build()
            );
        }
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
