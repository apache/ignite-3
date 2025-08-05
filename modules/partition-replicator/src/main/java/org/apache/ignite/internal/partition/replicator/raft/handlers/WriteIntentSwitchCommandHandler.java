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

package org.apache.ignite.internal.partition.replicator.raft.handlers;

import java.util.function.IntFunction;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommandV2;
import org.apache.ignite.internal.partition.replicator.raft.CommandResult;
import org.apache.ignite.internal.partition.replicator.raft.RaftTableProcessor;
import org.apache.ignite.internal.partition.replicator.raft.RaftTxFinishMarker;
import org.apache.ignite.internal.tx.TxManager;
import org.jetbrains.annotations.Nullable;

/**
 * Handler for {@link WriteIntentSwitchCommand}s.
 */
public class WriteIntentSwitchCommandHandler extends AbstractCommandHandler<WriteIntentSwitchCommand> {
    private static final IgniteLogger LOG = Loggers.forClass(WriteIntentSwitchCommandHandler.class);

    private final IntFunction<RaftTableProcessor> tableProcessorByTableId;

    private final RaftTxFinishMarker txFinishMarker;

    /** Constructor. */
    public WriteIntentSwitchCommandHandler(IntFunction<RaftTableProcessor> tableProcessorByTableId, TxManager txManager) {
        this.tableProcessorByTableId = tableProcessorByTableId;

        txFinishMarker = new RaftTxFinishMarker(txManager);
    }

    @Override
    protected CommandResult handleInternally(
            WriteIntentSwitchCommand switchCommand,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        assert switchCommand instanceof WriteIntentSwitchCommandV2 : "Unexpected command type: " + switchCommand.getClass();

        txFinishMarker.markFinished(switchCommand.txId(), switchCommand.commit(), switchCommand.commitTimestamp(), null);

        boolean applied = false;
        for (int tableId : ((WriteIntentSwitchCommandV2) switchCommand).tableIds()) {
            RaftTableProcessor tableProcessor = raftTableProcessor(tableId);

            if (tableProcessor == null) {
                // This can only happen if the table in question has already been dropped and destroyed. In such case, we simply
                // don't need to do anything as the partition storage is already destroyed.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Table processor for table ID {} not found. Command execution for the table will be ignored: {}",
                            tableId, switchCommand.toStringForLightLogging());
                }

                continue;
            }

            CommandResult singleResult = tableProcessor
                    .processCommand(switchCommand, commandIndex, commandTerm, safeTimestamp);

            applied = applied || singleResult.wasApplied();
        }

        return new CommandResult(null, applied);
    }

    private @Nullable RaftTableProcessor raftTableProcessor(int tableId) {
        return tableProcessorByTableId.apply(tableId);
    }
}
