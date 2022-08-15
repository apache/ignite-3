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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.raft.client.service.RaftGroupService;

/**
 * Replica server.
 * TODO:IGNITE-17257 Implement Replica server-side logic.
 */
public class Replica {
    /** Replica group identity, this id is the same as the considered partition's id. */
    private final String replicaGrpId;

    /** Raft group service. */
    private final RaftGroupService raftGroupService;

    /** Replica listener. */
    private final ReplicaListener listener;

    /**
     * The constructor of replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param raftGroupService Raft group service.
     * @param listener Replica listener.
     */
    public Replica(
            String replicaGrpId,
            RaftGroupService raftGroupService,
            ReplicaListener listener
    ) {
        this.replicaGrpId = replicaGrpId;
        this.raftGroupService = raftGroupService;
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
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]",
                request.groupId(),
                replicaGrpId);

        return raftGroupService.refreshAndGetLeaderWithTerm()
                .thenCompose((replicaAndTerm) -> {
                    if (replicaAndTerm.get1().address().equals(request.primaryReplica().address())
                            && replicaAndTerm.get2().equals(request.term())) {
                        return listener.invoke(request);
                    } else {
                        return CompletableFuture.failedFuture(new PrimaryReplicaMissException(
                                request.primaryReplica().address(),
                                replicaAndTerm.get1().address(),
                                request.term(),
                                replicaAndTerm.get2()
                        ));
                    }
                });
    }
}
