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
import java.util.function.Function;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.raft.Command;

/**
 * Handler for {@link org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest}.
 */
public class MinimumActiveTxTimeReplicaRequestHandler {
    /** Factory to create RAFT command messages. */
    private final PartitionReplicationMessagesFactory partitionReplicationMessageFactory;

    /** Closure that applies RAFT command message that is created by this handler. */
    private final Function<Command, CompletableFuture<?>> commandProcessor;

    /** Clock service. */
    private final ClockService clockService;

    /**
     * Creates a new instance of MinimumActiveTxTimeReplicaRequestHandler.
     *
     * @param messageFactory Factory to create RAFT command messages.
     * @param clockService Clock service.
     * @param commandProcessor Closure that applies RAFT command message.
     */
    public MinimumActiveTxTimeReplicaRequestHandler(
            PartitionReplicationMessagesFactory messageFactory,
            ClockService clockService,
            Function<Command, CompletableFuture<?>> commandProcessor
    ) {
        this.partitionReplicationMessageFactory = messageFactory;
        this.clockService = clockService;
        this.commandProcessor = commandProcessor;
    }

    /**
     * Handles {@link org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest}.
     *
     * @param request Request to handle.
     * @return Future that will be completed when the request is handled.
     */
    public CompletableFuture<?> handle(UpdateMinimumActiveTxBeginTimeReplicaRequest request) {
        Command cmd = partitionReplicationMessageFactory.updateMinimumActiveTxBeginTimeCommand()
                .timestamp(request.timestamp())
                .initiatorTime(clockService.now())
                .build();

        // The timestamp must increase monotonically, otherwise it will have to be
        // stored on disk so that reordering does not occur after the node is restarted.
        return commandProcessor.apply(cmd);
    }
}
