/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.replicator;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * Replica server.
 * TODO:IGNITE-17257 Implement Replica server-side logic.
 */
public class Replica {
    /** Replica group identity, this id is the same as the considered partition's id. */
    private final String replicaGrpId;

    /** Replica listener. */
    private final ReplicaListener listener;

    /**
     * The map matches an operation id to the future of operation result.
     * The first id is a business transaction id within which the operation is handled.
     * The second one is an id of operation (the id is locally generated when the replication request is received).
     * Operation future is a leaf element for waiting the operation completion and receiving a result.
     */
    private final ConcurrentHashMap<UUID, ConcurrentHashMap<UUID, CompletableFuture>> ops = new ConcurrentHashMap<>();

    /**
     * The constructor of replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     */
    public Replica(
            String replicaGrpId,
            ReplicaListener listener
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
    }

    /**
     * Process a replication request on the replica.
     *
     * @param request Request to replication.
     * @return Response.
     */
    public CompletableFuture<Object> processRequest(ReplicaRequest request) { // define proper set of exceptions that might be thrown.
        assert replicaGrpId.equals(request.groupId()) : IgniteStringFormatter.format(
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]", request.groupId(),
                replicaGrpId);

        //TODO:IGNITE-17378 Check replica is alive.

        return listener.invoke(request);
    }
}
