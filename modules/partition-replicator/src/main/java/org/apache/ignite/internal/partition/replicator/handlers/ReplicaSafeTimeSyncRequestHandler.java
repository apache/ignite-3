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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.partition.replicator.ReplicationRaftCommandApplicator;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;

/**
 * Handler for {@link ReplicaSafeTimeSyncRequest}.
 */
public class ReplicaSafeTimeSyncRequestHandler {
    /** Factory for creating replica command messages. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Applicator that applies RAFT command that is created by this handler. */
    private final ReplicationRaftCommandApplicator commandApplicator;

    /** Clock service. */
    private final ClockService clockService;

    /**
     * Creates a new instance of ReplicaSafeTimeSyncRequestHandler.
     *
     * @param clockService Clock service.
     * @param commandApplicator Applicator that applies RAFT command that is created by this handler.
     */
    public ReplicaSafeTimeSyncRequestHandler(
            ClockService clockService,
            ReplicationRaftCommandApplicator commandApplicator
    ) {
        this.clockService = clockService;
        this.commandApplicator = commandApplicator;
    }

    /**
     * Handles {@link ReplicaSafeTimeSyncRequest}.
     *
     * @param isPrimary Whether current node is a primary replica.
     * @return Future that will be completed when the request is handled.
     */
    public CompletableFuture<?> handle(boolean isPrimary) {
        if (!isPrimary) {
            return nullCompletedFuture();
        }

        return commandApplicator.applyCommandWithExceptionHandling(
                REPLICA_MESSAGES_FACTORY.safeTimeSyncCommand().initiatorTime(clockService.now()).build()
        );
    }
}
