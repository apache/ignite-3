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
import static org.apache.ignite.internal.tx.message.TxMessageGroup.VACUUM_TX_STATE_COMMAND;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
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
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.jetbrains.annotations.Nullable;

/**
 * RAFT listener for the zone partition.
 */
public class ZonePartitionRaftListener implements RaftGroupListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionRaftListener.class);

    private final SafeTimeValuesTracker safeTimeTracker;

    private final PendingComparableValuesTracker<Long, Void> storageIndexTracker;

    /** Mapping table ID to table request processor. */
    private final Map<Integer, RaftTableProcessor> tableProcessors = new ConcurrentHashMap<>();

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
            PartitionsSnapshots partitionsSnapshots
    ) {
        this.safeTimeTracker = safeTimeTracker;
        this.storageIndexTracker = storageIndexTracker;
        this.partitionsSnapshots = partitionsSnapshots;
        this.txStateStorage = txStatePartitionStorage;
        this.partitionKey = new ZonePartitionKey(zonePartitionId.zoneId(), zonePartitionId.partitionId());

        onSnapshotSaveHandler = new OnSnapshotSaveHandler(txStatePartitionStorage);

        // RAFT command handlers initialization.
        this.commandHandlers = new CommandHandlers.Builder()
                .addHandler(
                        PartitionReplicationMessageGroup.GROUP_TYPE,
                        Commands.FINISH_TX,
                        new FinishTxCommandHandler(txStatePartitionStorage, zonePartitionId, txManager))
                .addHandler(
                        PartitionReplicationMessageGroup.GROUP_TYPE,
                        Commands.WRITE_INTENT_SWITCH,
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
                LOG.error(
                        "Unknown error while processing command [commandIndex={}, commandTerm={}, command={}]",
                        t,
                        clo.index(), clo.index(), clo.command()
                );

                clo.result(t);

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

        IgniteBiTuple<Serializable, Boolean> result;

        // NB: Make sure that ANY command we accept here updates lastAppliedIndex+term info in one of the underlying
        // storages!
        // Otherwise, a gap between lastAppliedIndex from the point of view of JRaft and our storage might appear.
        // If a leader has such a gap, and does doSnapshot(), it will subsequently truncate its log too aggressively
        // in comparison with 'snapshot' state stored in our storages; and if we install a snapshot from our storages
        // to a follower at this point, for a subsequent AppendEntries the leader will not be able to get prevLogTerm
        // (because it's already truncated in the leader's log), so it will have to install a snapshot again, and then
        // repeat same thing over and over again.

        partitionSnapshots().acquireReadLock();

        try {
            boolean crossTableCommand = command instanceof UpdateMinimumActiveTxBeginTimeCommand;

            if (command instanceof PrimaryReplicaChangeCommand) {
                // This is a hack for tests, this command is not issued in production because no zone-wide placement driver exists yet.
                // FIXME: https://issues.apache.org/jira/browse/IGNITE-24374
                result = processCrossTableProcessorsCommand(command, commandIndex, commandTerm, safeTimestamp);

                if (commandIndex > txStateStorage.lastAppliedIndex()) {
                    var primaryReplicaChangeCommand = (PrimaryReplicaChangeCommand) command;

                    var leaseInfo = new LeaseInfo(
                            primaryReplicaChangeCommand.leaseStartTime(),
                            primaryReplicaChangeCommand.primaryReplicaNodeId(),
                            primaryReplicaChangeCommand.primaryReplicaNodeName()
                    );

                    txStateStorage.leaseInfo(leaseInfo, commandIndex, commandTerm);

                    result = new IgniteBiTuple<>(null, true);
                }
            } else if (command instanceof TableAwareCommand) {
                result = processTableAwareCommand(
                        ((TableAwareCommand) command).tableId(),
                        command,
                        commandIndex,
                        commandTerm,
                        safeTimestamp
                );
            } else if (crossTableCommand) {
                result = processCrossTableProcessorsCommand(command, commandIndex, commandTerm, safeTimestamp);
            } else {
                AbstractCommandHandler<?> commandHandler =
                        commandHandlers.handler(command.groupType(), command.messageType());

                if (commandHandler == null) {
                    LOG.info("Message type " + command.getClass() + " is not supported by the zone partition RAFT listener yet");

                    result = new IgniteBiTuple<>(null, true);
                } else {
                    result = commandHandler.handle(command, commandIndex, commandTerm, safeTimestamp);
                }
            }

            if (Boolean.TRUE.equals(result.get2())) {
                // Adjust safe time before completing update to reduce waiting.
                if (safeTimestamp != null) {
                    updateTrackerIgnoringTrackerClosedException(safeTimeTracker, safeTimestamp);
                }

                updateTrackerIgnoringTrackerClosedException(storageIndexTracker, clo.index());
            }

            synchronized (tableProcessorsStateLock) {
                lastAppliedIndex = max(lastAppliedIndex, commandIndex);
                lastAppliedTerm = max(lastAppliedTerm, commandTerm);
            }
        } finally {
            partitionSnapshots().releaseReadLock();
        }

        // Completing the closure out of the partition snapshots lock to reduce possibility of deadlocks as it might
        // trigger other actions taking same locks.
        clo.result(result.get1());
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
    private IgniteBiTuple<Serializable, Boolean> processCrossTableProcessorsCommand(
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        IgniteBiTuple<Serializable, Boolean> result = new IgniteBiTuple<>(null, false);

        tableProcessors.values().forEach(processor -> {
            IgniteBiTuple<Serializable, Boolean> r = processor.processCommand(command, commandIndex, commandTerm, safeTimestamp);
            // Need to adjust the safe time if any of the table processors successfully handled the command.
            if (Boolean.TRUE.equals(r.get2())) {
                result.set2(Boolean.TRUE);
            }
        });

        return result;
    }

    /**
     * Redirects the command to a particular raft table processor for the given {@code tablePartitionId}.
     *
     * @param tableId ID of a table.
     * @param command Command to process.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     * @param safeTimestamp Safe timestamp.
     * @return Tuple with the result of the command processing and a flag indicating whether the command was applied.
     */
    private IgniteBiTuple<Serializable, Boolean> processTableAwareCommand(
            int tableId,
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        return tableProcessors.get(tableId).processCommand(command, commandIndex, commandTerm, safeTimestamp);
    }

    @Override
    public void onConfigurationCommitted(RaftGroupConfiguration config, long lastAppliedIndex, long lastAppliedTerm) {
        synchronized (tableProcessorsStateLock) {
            tableProcessors.values()
                    .forEach(listener -> listener.onConfigurationCommitted(config, lastAppliedIndex, lastAppliedTerm));

            byte[] configBytes = raftGroupConfigurationConverter.toBytes(config);

            txStateStorage.committedGroupConfiguration(configBytes, lastAppliedIndex, lastAppliedTerm);

            this.lastAppliedIndex = max(this.lastAppliedIndex, lastAppliedIndex);
            this.lastAppliedTerm = max(this.lastAppliedTerm, lastAppliedTerm);
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        long lastAppliedIndex;
        long lastAppliedTerm;

        synchronized (tableProcessorsStateLock) {
            lastAppliedIndex = this.lastAppliedIndex;
            lastAppliedTerm = this.lastAppliedTerm;
        }

        onSnapshotSaveHandler.onSnapshotSave(lastAppliedIndex, lastAppliedTerm, tableProcessors.values())
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        synchronized (tableProcessorsStateLock) {
            lastAppliedIndex = max(lastAppliedIndex, txStateStorage.lastAppliedIndex());
            lastAppliedTerm = max(lastAppliedTerm, txStateStorage.lastAppliedTerm());
        }

        return true;
    }

    @Override
    public void onShutdown() {
        cleanupSnapshots();

        tableProcessors.values().forEach(RaftTableProcessor::onShutdown);
    }

    /**
     * Adds a given Table Partition-level Raft processor to the set of managed processor.
     *
     * <p>Callers of this method must ensure that no commands are issued to this processor before this method returns.
     *
     * <p>During table creation this is achieved by executing this method while processing a Catalog event, which blocks bumping the Catalog
     * version. Until the Catalog version is updated, commands targeting the table being added will be rejected by an interceptor that
     * requires the Catalog version to be equal to a particular value.
     */
    public void addTableProcessor(TablePartitionId tablePartitionId, RaftTableProcessor processor) {
        synchronized (tableProcessorsStateLock) {
            RaftGroupConfiguration configuration = raftGroupConfigurationConverter.fromBytes(txStateStorage.committedGroupConfiguration());

            LeaseInfo leaseInfo = txStateStorage.leaseInfo();

            processor.initialize(configuration, leaseInfo, lastAppliedIndex, lastAppliedTerm);

            RaftTableProcessor prev = tableProcessors.put(tablePartitionId.tableId(), processor);

            assert prev == null : "Listener for table partition " + tablePartitionId + " already exists";
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

    private void cleanupSnapshots() {
        partitionsSnapshots.cleanupOutgoingSnapshots(partitionKey);
    }

    private PartitionSnapshots partitionSnapshots() {
        return partitionsSnapshots.partitionSnapshots(partitionKey);
    }
}
