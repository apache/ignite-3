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

import java.io.Serializable;
import java.util.function.IntFunction;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.raft.RaftTableProcessor;
import org.apache.ignite.internal.partition.replicator.raft.RaftTxFinishMarker;
import org.apache.ignite.internal.tx.TxManager;
import org.jetbrains.annotations.Nullable;

/**
 * Handler for {@link WriteIntentSwitchCommand}s.
 */
public class WriteIntentSwitchCommandHandler extends AbstractCommandHandler<WriteIntentSwitchCommand> {
    private final IntFunction<RaftTableProcessor> tableProcessorByTableId;

    private final RaftTxFinishMarker txFinishMarker;

    /** Constructor. */
    public WriteIntentSwitchCommandHandler(IntFunction<RaftTableProcessor> tableProcessorByTableId, TxManager txManager) {
        this.tableProcessorByTableId = tableProcessorByTableId;

        txFinishMarker = new RaftTxFinishMarker(txManager);
    }

    @Override
    protected IgniteBiTuple<Serializable, Boolean> handleInternally(
            WriteIntentSwitchCommand switchCommand,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        txFinishMarker.markFinished(switchCommand.txId(), switchCommand.commit(), switchCommand.commitTimestamp(), null);

        boolean applied = false;
        for (int tableId : switchCommand.tableIds()) {
            IgniteBiTuple<Serializable, Boolean> singleResult = raftTableProcessor(tableId)
                    .processCommand(switchCommand, commandIndex, commandTerm, safeTimestamp);
            if (singleResult.get2()) {
                applied = true;
            }
        }

        return new IgniteBiTuple<>(null, applied);
    }

    private RaftTableProcessor raftTableProcessor(int tableId) {
        RaftTableProcessor raftTableProcessor = tableProcessorByTableId.apply(tableId);

        assert raftTableProcessor != null : "No RAFT table processor found by table ID " + tableId;

        return raftTableProcessor;
    }
}
