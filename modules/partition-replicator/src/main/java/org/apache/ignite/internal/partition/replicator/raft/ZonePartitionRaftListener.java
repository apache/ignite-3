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

import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.TableAwareCommand;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.CommittedConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.tx.TransactionResult;

/**
 * RAFT listener for the zone partition.
 */
public class ZonePartitionRaftListener implements RaftGroupListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionRaftListener.class);

    private final Map<TablePartitionId, RaftGroupListener> tablePartitionRaftListeners = new ConcurrentHashMap<>();

    /**
     * Latest committed configuration of the zone-wide Raft group.
     *
     * <p>Multi-threaded access is guarded by {@link #commitedConfigurationLock}.
     */
    private CommittedConfiguration currentCommitedConfiguration;

    private final Object commitedConfigurationLock = new Object();

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

        if (command instanceof FinishTxCommand) {
            FinishTxCommand cmd = (FinishTxCommand) command;

            clo.result(new TransactionResult(cmd.commit() ? COMMITTED : ABORTED, cmd.commitTimestamp()));
        } else if (command instanceof PrimaryReplicaChangeCommand) {
            // This is a hack for tests, this command is not issued in production because no zone-wide placement driver exists yet.
            CommandClosure<WriteCommand> idempotentCommandClosure = idempotentCommandClosure(clo);

            tablePartitionRaftListeners.values().forEach(listener -> listener.onWrite(singletonIterator(idempotentCommandClosure)));

            clo.result(null);
        } else if (command instanceof TableAwareCommand) {
            TablePartitionId tablePartitionId = ((TableAwareCommand) command).tablePartitionId().asTablePartitionId();

            processTableAwareCommand(tablePartitionId, clo);
        } else {
            LOG.info("Message type " + command.getClass() + " is not supported by the zone partition RAFT listener yet");

            clo.result(null);
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
    public void onConfigurationCommitted(CommittedConfiguration config) {
        synchronized (commitedConfigurationLock) {
            currentCommitedConfiguration = config;

            tablePartitionRaftListeners.values().forEach(listener -> listener.onConfigurationCommitted(config));
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
                listener.onConfigurationCommitted(currentCommitedConfiguration);
            }

            RaftGroupListener prev = tablePartitionRaftListeners.put(tablePartitionId, listener);

            assert prev == null : "Listener for table partition " + tablePartitionId + " already exists";
        }
    }
}
