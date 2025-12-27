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

package org.apache.ignite.internal.partition.replicator.raft;

import static java.lang.Math.max;
import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_APPLIED_RESULT;
import static org.apache.ignite.internal.tx.message.TxMessageGroup.VACUUM_TX_STATE_COMMAND;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.Commands;
import org.apache.ignite.internal.partition.replicator.network.command.TableAwareCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxBeginTimeCommand;
import org.apache.ignite.internal.partition.replicator.raft.handlers.AbstractCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.handlers.CommandHandlers;
import org.apache.ignite.internal.partition.replicator.raft.handlers.FinishTxCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.handlers.VacuumTxStatesCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.handlers.WriteIntentSwitchCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionsSnapshots;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * RAFT listener for the zone partition.
 */
public class ZonePartitionRaftListener implements RaftGroupListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionRaftListener.class);

    private final SafeTimeValuesTracker safeTimeTracker;

    private final PendingComparableValuesTracker<Long, Void> storageIndexTracker;

    /**
     * Mapping of table ID to table request processor.
     *
     * <p>Concurrent access is guarded by {@link #tableProcessorsStateLock}.
     */
    private final Int2ObjectMap<RaftTableProcessor> tableProcessors = new Int2ObjectOpenHashMap<>();

    private final TxStatePartitionStorage txStateStorage;

    private final PartitionsSnapshots partitionsSnapshots;

    private final PartitionKey partitionKey;

    /**
     * Last applied index across all table processors and {@link #txStateStorage}.
     *
     * <p>Multi-threaded access is guarded by {@link #tableProcessorsStateLock}.
     */
    private long lastAppliedIndex;

    /**
     * Last applied term across all table processors and {@link #txStateStorage}.
     *
     * <p>Multi-threaded access is guarded by {@link #tableProcessorsStateLock}.
     */
    private long lastAppliedTerm;

    private final Object tableProcessorsStateLock = new Object();

    private final OnSnapshotSaveHandler onSnapshotSaveHandler;

    // Raft command handlers.
    private final CommandHandlers commandHandlers;

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    /** Constructor. */
    public ZonePartitionRaftListener(
            ZonePartitionId zonePartitionId,
            TxStatePartitionStorage txStatePartitionStorage,
            TxManager txManager,
            SafeTimeValuesTracker safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            PartitionsSnapshots partitionsSnapshots,
            Executor partitionOperationsExecutor
    ) {
        this.safeTimeTracker = safeTimeTracker;
        this.storageIndexTracker = storageIndexTracker;
        this.partitionsSnapshots = partitionsSnapshots;
        this.txStateStorage = txStatePartitionStorage;
        this.partitionKey = new ZonePartitionKey(zonePartitionId.zoneId(), zonePartitionId.partitionId());

        onSnapshotSaveHandler = new OnSnapshotSaveHandler(txStatePartitionStorage, partitionOperationsExecutor);

        // RAFT command handlers initialization.
        this.commandHandlers = new CommandHandlers.Builder()
                .addHandler(
                        PartitionReplicationMessageGroup.GROUP_TYPE,
                        Commands.FINISH_TX_V2,
                        new FinishTxCommandHandler(txStatePartitionStorage, zonePartitionId, txManager))
                .addHandler(
                        PartitionReplicationMessageGroup.GROUP_TYPE,
                        Commands.WRITE_INTENT_SWITCH_V2,
                        new WriteIntentSwitchCommandHandler(tableProcessors::get, txManager))
                .addHandler(
                        TxMessageGroup.GROUP_TYPE,
                        VACUUM_TX_STATE_COMMAND,
                        new VacuumTxStatesCommandHandler(txStatePartitionStorage))
                .build();
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining(clo -> {
            Command command = clo.command();

            assert false : "No read commands expected, [cmd=" + command + ']';
        });
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        iterator.forEachRemaining(clo -> {
            try {
                processWriteCommand(clo);
            } catch (Throwable t) {
                clo.result(t);

                LOG.error(
                        "Failed to process write command [commandIndex={}, commandTerm={}, command={}]",
                        t,
                        clo.index(), clo.term(), clo.command()
                );

                throw t;
            }
        });
    }

    private void processWriteCommand(CommandClosure<WriteCommand> clo) {
        WriteCommand command = clo.command();

        long commandIndex = clo.index();
        long commandTerm = clo.term();
        @Nullable HybridTimestamp safeTimestamp = clo.safeTimestamp();
        assert safeTimestamp == null || command instanceof SafeTimePropagatingCommand : command;

        CommandResult result;

        // NB: Make sure that ANY command we accept here updates lastAppliedIndex+term info in one of the underlying
        // storages!
        // Otherwise, a gap between lastAppliedIndex from the point of view of JRaft and our storage might appear.
        // If a leader has such a gap, and does doSnapshot(), it will subsequently truncate its log too aggressively
        // in comparison with 'snapshot' state stored in our storages; and if we install a snapshot from our storages
        // to a follower at this point, for a subsequent AppendEntries the leader will not be able to get prevLogTerm
        // (because it's already truncated in the leader's log), so it will have to install a snapshot again, and then
        // repeat same thing over and over again.

        synchronized (tableProcessorsStateLock) {
            partitionSnapshots().acquireReadLock();

            try {
                if (command instanceof TableAwareCommand) {
                    result = processTableAwareCommand(
                            ((TableAwareCommand) command).tableId(),
                            command,
                            commandIndex,
                            commandTerm,
                            safeTimestamp
                    );
                } else if (command instanceof UpdateMinimumActiveTxBeginTimeCommand) {
                    result = processCrossTableProcessorsCommand(command, commandIndex, commandTerm, safeTimestamp);
                } else if (command instanceof SafeTimeSyncCommand) {
                    result = processCrossTableProcessorsCommand(command, commandIndex, commandTerm, safeTimestamp);
                } else if (command instanceof PrimaryReplicaChangeCommand) {
                    result = processCrossTableProcessorsCommand(command, commandIndex, commandTerm, safeTimestamp);

                    if (updateLeaseInfoInTxStorage((PrimaryReplicaChangeCommand) command, commandIndex, commandTerm)) {
                        result = EMPTY_APPLIED_RESULT;
                    }
                } else {
                    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Cleanup when all commands will be covered.
                    AbstractCommandHandler<?> commandHandler =
                            commandHandlers.handler(command.groupType(), command.messageType());

                    if (commandHandler == null) {
                        LOG.info("Message type {} is not supported by the zone partition RAFT listener yet", command.getClass());

                        result = EMPTY_APPLIED_RESULT;
                    } else {
                        result = commandHandler.handle(command, commandIndex, commandTerm, safeTimestamp);
                    }
                }

                if (result.wasApplied()) {
                    // Adjust safe time before completing update to reduce waiting.
                    if (safeTimestamp != null) {
                        try {
                            safeTimeTracker.update(safeTimestamp, commandIndex, commandTerm, command);
                        } catch (TrackerClosedException ignored) {
                            // Ignored.
                        }
                    }

                    try {
                        storageIndexTracker.update(commandIndex, null);
                    } catch (TrackerClosedException ignored) {
                        // Ignored.
                    }
                }

                lastAppliedIndex = max(lastAppliedIndex, commandIndex);
                lastAppliedTerm = max(lastAppliedTerm, commandTerm);
            } finally {
                partitionSnapshots().releaseReadLock();
            }
        }

        // Completing the closure out of the partition snapshots lock to reduce possibility of deadlocks as it might
        // trigger other actions taking same locks.
        clo.result(result.result());
    }

    /**
     * Redirects the command to all raft table processors to process.
     *
     * @param command Command to process.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     * @param safeTimestamp Safe timestamp.
     * @return Tuple with the result of the command processing and a flag indicating whether the command was applied.
     */
    private CommandResult processCrossTableProcessorsCommand(
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        if (tableProcessors.isEmpty()) {
            return new CommandResult(null, lastAppliedIndex < commandIndex);
        }

        boolean wasApplied = false;

        for (RaftTableProcessor processor : tableProcessors.values()) {
            CommandResult r = processor.processCommand(command, commandIndex, commandTerm, safeTimestamp);

            wasApplied = wasApplied || r.wasApplied();
        }

        return new CommandResult(null, wasApplied);
    }

    /**
     * Redirects the command to a particular raft table processor for the given {@code tablePartitionId}.
     *
     * @param tableId ID of a table.
     * @param command Command to process.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     * @param safeTimestamp Safe timestamp.
     * @return Result of the command processing.
     */
    private CommandResult processTableAwareCommand(
            int tableId,
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        RaftTableProcessor tableProcessor = tableProcessors.get(tableId);

        if (tableProcessor == null) {
            // Most of the times this condition should be false. This logging message is added in case a Raft command got stuck somewhere
            // while being replicated and arrived on this node after the target table had been removed. In this case we ignore the
            // command, which should be safe to do, because the underlying storage was destroyed anyway.
            LOG.warn("Table processor for table ID {} not found. Command will be ignored: {}",
                    tableId, command.toStringForLightLogging());

            return EMPTY_APPLIED_RESULT;
        }

        return tableProcessor.processCommand(command, commandIndex, commandTerm, safeTimestamp);
    }

    private boolean updateLeaseInfoInTxStorage(PrimaryReplicaChangeCommand command, long commandIndex, long commandTerm) {
        if (commandIndex <= txStateStorage.lastAppliedIndex()) {
            return false;
        }

        var leaseInfo = new LeaseInfo(
                command.leaseStartTime(),
                command.primaryReplicaNodeId(),
                command.primaryReplicaNodeName()
        );

        txStateStorage.leaseInfo(leaseInfo, commandIndex, commandTerm);

        return true;
    }

    @Override
    public void onConfigurationCommitted(RaftGroupConfiguration config, long lastAppliedIndex, long lastAppliedTerm) {
        synchronized (tableProcessorsStateLock) {
            partitionSnapshots().acquireReadLock();

            try {
                tableProcessors.values()
                        .forEach(listener -> listener.onConfigurationCommitted(config, lastAppliedIndex, lastAppliedTerm));
            } finally {
                partitionSnapshots().releaseReadLock();
            }

            byte[] configBytes = raftGroupConfigurationConverter.toBytes(config);

            txStateStorage.committedGroupConfiguration(configBytes, lastAppliedIndex, lastAppliedTerm);

            this.lastAppliedIndex = max(this.lastAppliedIndex, lastAppliedIndex);
            this.lastAppliedTerm = max(this.lastAppliedTerm, lastAppliedTerm);

            try {
                storageIndexTracker.update(lastAppliedIndex, null);
            } catch (TrackerClosedException ignored) {
                // Ignored.
            }
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        synchronized (tableProcessorsStateLock) {
            byte[] configuration = txStateStorage.committedGroupConfiguration();

            assert configuration != null : "Trying to create a snapshot without Raft group configuration";

            var snapshotInfo = new PartitionSnapshotInfo(
                    lastAppliedIndex,
                    lastAppliedTerm,
                    txStateStorage.leaseInfo(),
                    configuration,
                    tableProcessors.keySet()
            );

            onSnapshotSaveHandler.onSnapshotSave(snapshotInfo, tableProcessors.values())
                    .whenComplete((unused, throwable) -> doneClo.accept(throwable));
        }
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        synchronized (tableProcessorsStateLock) {
            lastAppliedIndex = max(lastAppliedIndex, txStateStorage.lastAppliedIndex());
            lastAppliedTerm = max(lastAppliedTerm, txStateStorage.lastAppliedTerm());

            storageIndexTracker.update(lastAppliedIndex, null);
        }

        return true;
    }

    @Override
    public void onShutdown() {
        cleanupSnapshots();

        synchronized (tableProcessorsStateLock) {
            tableProcessors.values().forEach(RaftTableProcessor::onShutdown);
        }
    }

    /**
     * Adds a given Table Partition-level Raft processor to the set of managed processors.
     *
     * <p>Callers of this method must ensure that no commands are issued to this processor before this method returns.
     *
     * <p>During table creation this is achieved by executing this method while processing a Catalog event, which blocks bumping the Catalog
     * version. Until the Catalog version is updated, commands targeting the table being added will be rejected by an interceptor that
     * requires the Catalog version to be equal to a particular value.
     */
    public void addTableProcessor(int tableId, RaftTableProcessor processor) {
        synchronized (tableProcessorsStateLock) {
            RaftGroupConfiguration configuration = raftGroupConfigurationConverter.fromBytes(txStateStorage.committedGroupConfiguration());

            LeaseInfo leaseInfo = txStateStorage.leaseInfo();

            processor.initialize(configuration, leaseInfo, lastAppliedIndex, lastAppliedTerm);

            RaftTableProcessor prev = tableProcessors.put(tableId, processor);

            assert prev == null : "Listener for table " + tableId + " already exists";
        }
    }

    /**
     * Adds a given Table Partition-level Raft processor to the set of managed processors during node recovery on startup.
     */
    public void addTableProcessorOnRecovery(int tableId, RaftTableProcessor processor) {
        synchronized (tableProcessorsStateLock) {
            PartitionSnapshotInfo snapshotInfo = snapshotInfo();

            // This method is called on node recovery in order to add existing table processors to a zone being started. During recovery
            // we may encounter "empty" storages, i.e. storages that were created when the node was running but didn't have the chance
            // to durably flush their data on disk. In this case we use the last saved Raft snapshot information to determine if a given
            // storage has ever participated in a Raft snapshot.
            // - If it has, then it is expected to have a persistent state and, since this storage is currently empty, it must have
            // experienced a disaster. In this case, we do not initialize the given processor and the node recovery code will try to
            // replay the Raft log from the beginning of time, if it's available, or will throw an error.
            // - If it has not, then it is guaranteed that this storage has never had any updates before the last snapshot and we can use
            // its state to initialize the storage. Any missed updates that may have come after the snapshot will be re-applied as a part
            // of Raft log replay.
            if (snapshotInfo != null && !snapshotInfo.tableIds().contains(tableId)) {
                RaftGroupConfiguration configuration = raftGroupConfigurationConverter.fromBytes(snapshotInfo.configurationBytes());

                processor.initialize(
                        configuration,
                        snapshotInfo.leaseInfo(),
                        snapshotInfo.lastAppliedIndex(),
                        snapshotInfo.lastAppliedTerm()
                );
            }

            RaftTableProcessor prev = tableProcessors.put(tableId, processor);

            assert prev == null : "Listener for table " + tableId + " already exists";
        }
    }

    private @Nullable PartitionSnapshotInfo snapshotInfo() {
        byte[] snapshotInfoBytes = txStateStorage.snapshotInfo();

        if (snapshotInfoBytes == null) {
            return null;
        }

        return VersionedSerialization.fromBytes(snapshotInfoBytes, PartitionSnapshotInfoSerializer.INSTANCE);
    }

    /** Returns the table processor associated with the given table ID. */
    @TestOnly
    public @Nullable RaftTableProcessor tableProcessor(int tableId) {
        synchronized (tableProcessorsStateLock) {
            return tableProcessors.get(tableId);
        }
    }

    /**
     * Removes a given Table Partition-level Raft processor from the set of managed processors.
     *
     * @param tableId Table's identifier.
     */
    public void removeTableProcessor(int tableId) {
        synchronized (tableProcessorsStateLock) {
            tableProcessors.remove(tableId);
        }
    }

    /**
     * Returns true if there are no table processors, false otherwise.
     */
    public boolean areTableRaftProcessorsEmpty() {
        synchronized (tableProcessorsStateLock) {
            return tableProcessors.isEmpty();
        }
    }

    private void cleanupSnapshots() {
        partitionsSnapshots.cleanupOutgoingSnapshots(partitionKey);
    }

    private PartitionSnapshots partitionSnapshots() {
        return partitionsSnapshots.partitionSnapshots(partitionKey);
    }

    @TestOnly
    public HybridTimestamp currentSafeTime() {
        return safeTimeTracker.current();
    }
}
