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
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.TableAwareCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxBeginTimeCommand;
import org.apache.ignite.internal.partition.replicator.raft.handlers.FinishTxCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionsSnapshots;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.tx.TxManager;
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

    /** Mapping table partition identifier to table request processor. */
    private final Map<TablePartitionId, RaftTableProcessor> tableProcessors = new ConcurrentHashMap<>();

    private final PartitionsSnapshots partitionsSnapshots;

    private final PartitionKey partitionKey;

    /**
     * Latest committed configuration of the zone-wide Raft group.
     *
     * <p>Multi-threaded access is guarded by {@link #commitedConfigurationLock}.
     */
    private CommittedConfiguration currentCommitedConfiguration;

    private final Object commitedConfigurationLock = new Object();

    // Raft command handlers.
    private final FinishTxCommandHandler finishTxCommandHandler;

    private final OnSnapshotSaveHandler onSnapshotSaveHandler;

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
        this.partitionKey = new ZonePartitionKey(zonePartitionId.zoneId(), zonePartitionId.partitionId());

        // RAFT command handlers initialization.
        finishTxCommandHandler = new FinishTxCommandHandler(
                txStatePartitionStorage,
                // TODO: IGNITE-24343 - use ZonePartitionId here.
                new TablePartitionId(zonePartitionId.zoneId(), zonePartitionId.partitionId()),
                txManager
        );

        onSnapshotSaveHandler = new OnSnapshotSaveHandler(txStatePartitionStorage, storageIndexTracker);
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
            if (command instanceof FinishTxCommand) {
                result = finishTxCommandHandler.handle((FinishTxCommand) command, commandIndex, commandTerm);
            } else if (command instanceof PrimaryReplicaChangeCommand) {
                // This is a hack for tests, this command is not issued in production because no zone-wide placement driver exists yet.
                // FIXME: https://issues.apache.org/jira/browse/IGNITE-24374
                tableProcessors.values().forEach(listener -> listener.processCommand(command, commandIndex, commandTerm, safeTimestamp));

                result = new IgniteBiTuple<>(null, true);
            } else if (command instanceof TableAwareCommand) {
                TablePartitionId tablePartitionId = ((TableAwareCommand) command).tablePartitionId().asTablePartitionId();

                result = processTableAwareCommand(tablePartitionId, command, commandIndex, commandTerm, safeTimestamp);
            } else if (command instanceof UpdateMinimumActiveTxBeginTimeCommand) {
                result = new IgniteBiTuple<>(null, false);

                tableProcessors.values().forEach(processor -> {
                    IgniteBiTuple<Serializable, Boolean> r = processor.processCommand(command, commandIndex, commandTerm, safeTimestamp);
                    // Need to adjust the safe time if any of the table processors successfully handled the command.
                    if (Boolean.TRUE.equals(r.get2())) {
                        result.set2(Boolean.TRUE);
                    }
                });
            } else {
                LOG.info("Message type " + command.getClass() + " is not supported by the zone partition RAFT listener yet");

                result = new IgniteBiTuple<>(null, true);
            }

            if (Boolean.TRUE.equals(result.get2())) {
                // Adjust safe time before completing update to reduce waiting.
                if (safeTimestamp != null) {
                    updateTrackerIgnoringTrackerClosedException(safeTimeTracker, safeTimestamp);
                }

                updateTrackerIgnoringTrackerClosedException(storageIndexTracker, clo.index());
            }
        } finally {
            partitionSnapshots().releaseReadLock();
        }

        // Completing the closure out of the partition snapshots lock to reduce possibility of deadlocks as it might
        // trigger other actions taking same locks.
        clo.result(result.get1());
    }

    private IgniteBiTuple<Serializable, Boolean> processTableAwareCommand(
            TablePartitionId tablePartitionId,
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        return tableProcessors.get(tablePartitionId).processCommand(command, commandIndex, commandTerm, safeTimestamp);
    }

    @Override
    public void onConfigurationCommitted(RaftGroupConfiguration config, long lastAppliedIndex, long lastAppliedTerm) {
        synchronized (commitedConfigurationLock) {
            currentCommitedConfiguration = new CommittedConfiguration(config, lastAppliedIndex, lastAppliedTerm);

            tableProcessors.values()
                    .forEach(listener -> listener.onConfigurationCommitted(config, lastAppliedIndex, lastAppliedTerm));
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        onSnapshotSaveHandler.onSnapshotSave(tableProcessors.values())
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        return true;
    }

    @Override
    public void onShutdown() {
        cleanupSnapshots();

        tableProcessors.values().forEach(RaftTableProcessor::onShutdown);
    }

    /**
     * Adds a given Table Partition-level Raft processor to the set of managed processor.
     */
    public void addTableProcessor(TablePartitionId tablePartitionId, RaftTableProcessor processor) {
        synchronized (commitedConfigurationLock) {
            if (currentCommitedConfiguration != null) {
                processor.onConfigurationCommitted(
                        currentCommitedConfiguration.configuration,
                        currentCommitedConfiguration.lastAppliedIndex,
                        currentCommitedConfiguration.lastAppliedTerm
                );
            }

            RaftTableProcessor prev = tableProcessors.put(tablePartitionId, processor);

            assert prev == null : "Listener for table partition " + tablePartitionId + " already exists";
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

    private static class CommittedConfiguration {
        final RaftGroupConfiguration configuration;

        final long lastAppliedIndex;

        final long lastAppliedTerm;

        CommittedConfiguration(RaftGroupConfiguration configuration, long lastAppliedIndex, long lastAppliedTerm) {
            this.configuration = configuration;
            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;
        }
    }
}
