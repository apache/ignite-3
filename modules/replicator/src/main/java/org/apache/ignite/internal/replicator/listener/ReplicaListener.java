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

package org.apache.ignite.internal.replicator.listener;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;

/** Replica listener. */
public interface ReplicaListener {
    /**
     * Invokes a replica listener to process request.
     *
     * @param request Replica request.
     * @param senderId Sender id.
     * @return Listener response.
     */
    CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, String senderId);

    /** Returns Raft-client. */
    RaftCommandRunner raftClient();

    /** Callback on replica shutdown. */
    default void onShutdown() {
        // No-op.
    }
}
