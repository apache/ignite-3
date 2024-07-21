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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ZonePartitionReplicaImpl;
import org.apache.ignite.internal.tx.TransactionResult;

/**
 * RAFT listener for the zone partition.
 */
public class ZonePartitionRaftListener implements RaftGroupListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionReplicaImpl.class);

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        // No-op
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends WriteCommand> clo) -> {
            Command command = clo.command();

            Serializable result = null;

            try {
                if (command instanceof FinishTxCommand) {
                    FinishTxCommand cmd = (FinishTxCommand) command;

                    result = new TransactionResult(cmd.commit() ? COMMITTED : ABORTED, cmd.commitTimestamp());
                } else {
                    LOG.debug("Message type " + command.getClass() + " is not supported by the zone partition RAFT listener yet");
                }
            } catch (IgniteInternalException e) {
                result = e;
            } catch (CompletionException e) {
                result = e.getCause();
            } catch (Throwable t) {
                LOG.error(
                        "Unknown error while processing command [commandIndex={}, commandTerm={}, command={}]",
                        t,
                        clo.index(), clo.index(), command
                );

                throw t;
            }

            clo.result(result);
        });
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        throw new UnsupportedOperationException("Snapshotting is not implemented");
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        throw new UnsupportedOperationException("Snapshotting is not implemented");
    }

    @Override
    public void onShutdown() {
        // No-op
    }
}
