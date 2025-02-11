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
import java.util.Collections;
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
import org.apache.ignite.internal.partition.replicator.raft.handlers.MinimumActiveTxTimeCommandHandler;
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
    private final Map<TablePartitionId, RaftGroupListener> tablePartitionRaftListeners = new ConcurrentHashMap<>();

    /**
     * Latest committed configuration of the zone-wide Raft group.
     *
     * <p>Multi-threaded access is guarded by {@link #commitedConfigurationLock}.
     */
    private CommittedConfiguration currentCommitedConfiguration;

    private final Object commitedConfigurationLock = new Object();

    // Raft command handlers.
    private final FinishTxCommandHandler finishTxCommandHandler;

    private final MinimumActiveTxTimeCommandHandler minimumActiveTxTimeCommandHandler;

    /** Constructor. */
    public ZonePartitionRaftListener(
            ZonePartitionId zonePartitionId,
            TxStatePartitionStorage txStatePartitionStorage,
            TxManager txManager,
            SafeTimeValuesTracker safeTimeTracker,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            MinimumRequiredTimeCollectorService minTimeCollectorService
    ) {
        this.safeTimeTracker = safeTimeTracker;
        this.storageIndexTracker = storageIndexTracker;

        // RAFT command handlers initialization.
        finishTxCommandHandler = new FinishTxCommandHandler(
                txStatePartitionStorage,
                // TODO: IGNITE-24343 - use ZonePartitionId here.
                new TablePartitionId(zonePartitionId.zoneId(), zonePartitionId.partitionId()),
                txManager
        );

        minimumActiveTxTimeCommandHandler = new MinimumActiveTxTimeCommandHandler(minTimeCollectorService);
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
        Command command = clo.command();

        long commandIndex = clo.index();
        long commandTerm = clo.term();
        @Nullable HybridTimestamp safeTimestamp = clo.safeTimestamp();
        assert safeTimestamp == null || command instanceof SafeTimePropagatingCommand : command;

        IgniteBiTuple<Serializable, Boolean> result = null;

        if (command instanceof FinishTxCommand) {
            result = finishTxCommandHandler.handle((FinishTxCommand) command, commandIndex, commandTerm);
        } else if (command instanceof PrimaryReplicaChangeCommand) {
            // This is a hack for tests, this command is not issued in production because no zone-wide placement driver exists yet.
            // FIXME: https://issues.apache.org/jira/browse/IGNITE-24374
            CommandClosure<WriteCommand> idempotentCommandClosure = idempotentCommandClosure(clo);

            tablePartitionRaftListeners.values().forEach(listener -> listener.onWrite(singletonIterator(idempotentCommandClosure)));

            result = new IgniteBiTuple<>(null, true);
        } else if (command instanceof TableAwareCommand) {
            TablePartitionId tablePartitionId = ((TableAwareCommand) command).tablePartitionId().asTablePartitionId();

            processTableAwareCommand(tablePartitionId, clo);
        } else if (command instanceof UpdateMinimumActiveTxBeginTimeCommand) {
            // TODO Adjust safe time before completing update to reduce waiting.
            tablePartitionRaftListeners.entrySet().forEach(entry -> {
                minimumActiveTxTimeCommandHandler.handle(
                        (UpdateMinimumActiveTxBeginTimeCommand) command,
                        commandIndex,
                        entry.getValue(),
                        entry.getKey());
            });

            // TODO adjust safetime
            clo.result(null);
        } else {
            LOG.info("Message type " + command.getClass() + " is not supported by the zone partition RAFT listener yet");

            clo.result(null);
        }

        // result == null means that the command either was not handled by anyone (and clo.result() is called) or
        // that it was delegated to a table processor (which called clo.result()).
        if (result != null) {
            if (Boolean.TRUE.equals(result.get2())) {
                // Adjust safe time before completing update to reduce waiting.
                if (safeTimestamp != null) {
                    updateTrackerIgnoringTrackerClosedException(safeTimeTracker, safeTimestamp);
                }

                updateTrackerIgnoringTrackerClosedException(storageIndexTracker, clo.index());
            }

            // Completing the closure out of the partition snapshots lock to reduce possibility of deadlocks as it might
            // trigger other actions taking same locks.
            clo.result(result.get1());
        }
    }

    private void processTableAwareCommand(TablePartitionId tablePartitionId, CommandClosure<WriteCommand> clo) {
        tablePartitionRaftListeners.get(tablePartitionId).onWrite(singletonIterator(clo));
    }

    private static <T> Iterator<T> singletonIterator(T value) {
        return Collections.singleton(value).iterator();
    }

    private static CommandClosure<WriteCommand> idempotentCommandClosure(CommandClosure<WriteCommand> clo) {
        return new CommandClosure<>() {
            @Override
            public WriteCommand command() {
                return clo.command();
            }

            @Override
            public long index() {
                return clo.index();
            }

            @Override
            public long term() {
                return clo.term();
            }

            @Override
            public void result(Serializable res) {
            }
        };
    }

    @Override
    public void onConfigurationCommitted(RaftGroupConfiguration config, long lastAppliedIndex, long lastAppliedTerm) {
        synchronized (commitedConfigurationLock) {
            currentCommitedConfiguration = new CommittedConfiguration(config, lastAppliedIndex, lastAppliedTerm);

            tablePartitionRaftListeners.values()
                    .forEach(listener -> listener.onConfigurationCommitted(config, lastAppliedIndex, lastAppliedTerm));
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        // TODO: implement, see https://issues.apache.org/jira/browse/IGNITE-22416
        throw new UnsupportedOperationException("Snapshotting is not implemented");
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        // TODO: implement, see https://issues.apache.org/jira/browse/IGNITE-22416
        throw new UnsupportedOperationException("Snapshotting is not implemented");
    }

    @Override
    public void onShutdown() {
        tablePartitionRaftListeners.values().forEach(RaftGroupListener::onShutdown);
    }

    /**
     * Adds a given Table Partition-level Raft listener to the set of managed listeners.
     */
    public void addTablePartitionRaftListener(TablePartitionId tablePartitionId, RaftGroupListener listener) {
        synchronized (commitedConfigurationLock) {
            if (currentCommitedConfiguration != null) {
                listener.onConfigurationCommitted(
                        currentCommitedConfiguration.configuration,
                        currentCommitedConfiguration.lastAppliedIndex,
                        currentCommitedConfiguration.lastAppliedTerm
                );
            }

            RaftGroupListener prev = tablePartitionRaftListeners.put(tablePartitionId, listener);

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
