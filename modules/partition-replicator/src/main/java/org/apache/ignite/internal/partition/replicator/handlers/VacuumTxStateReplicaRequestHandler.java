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

package org.apache.ignite.internal.partition.replicator.handlers;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.partition.replicator.ReplicationRaftCommandApplicator;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.VacuumTxStateReplicaRequest;
import org.apache.ignite.internal.tx.message.VacuumTxStatesCommand;

/**
 * Handler for {@link VacuumTxStateReplicaRequest}.
 */
public class VacuumTxStateReplicaRequestHandler {
    /** Factory for creating transaction command messages. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Applicator that applies RAFT command that is created by this handler. */
    private final ReplicationRaftCommandApplicator commandApplicator;

    /**
     * Creates a new instance of {@link VacuumTxStateReplicaRequestHandler}.
     *
     * @param commandApplicator Applicator that applies RAFT command that is created by this handler.
     */
    public VacuumTxStateReplicaRequestHandler(ReplicationRaftCommandApplicator commandApplicator) {
        this.commandApplicator = commandApplicator;
    }

    /**
     * Handles {@link VacuumTxStateReplicaRequest}.
     *
     * @param request Request to handle.
     * @return Future that will be completed when the request is handled.
     */
    public CompletableFuture<?> handle(VacuumTxStateReplicaRequest request) {
        VacuumTxStatesCommand cmd = TX_MESSAGES_FACTORY.vacuumTxStatesCommand()
                .txIds(request.transactionIds())
                .build();

        return commandApplicator.applyCommand(cmd);
    }
}
