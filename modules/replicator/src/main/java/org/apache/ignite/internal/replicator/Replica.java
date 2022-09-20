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

package org.apache.ignite.internal.replicator;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.SafeTimestampAware;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * Replica server.
 */
public class Replica {
    /** Replica group identity, this id is the same as the considered partition's id. */
    private final String replicaGrpId;

    /** Replica listener. */
    private final ReplicaListener listener;

    private final HybridClock safeTimeClock;

    /**
     * The constructor of a replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param safeTimeClock
     */
    public Replica(
            String replicaGrpId,
            ReplicaListener listener,
            HybridClock safeTimeClock
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.safeTimeClock = safeTimeClock;
    }

    /**
     * Processes a replication request on the replica.
     *
     * @param request Request to replication.
     * @return Response.
     */
    public CompletableFuture<Object> processRequest(ReplicaRequest request) {
        assert replicaGrpId.equals(request.groupId()) : IgniteStringFormatter.format(
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]",
                request.groupId(),
                replicaGrpId);

        if (request instanceof SafeTimestampAware) {
            SafeTimestampAware timestampAwareRequest = (SafeTimestampAware) request;

            syncSafeTimestamp(timestampAwareRequest.safeTimestamp());
        }

        return listener.invoke(request);
    }

    /**
     * Safe time for this replica.
     *
     * @return Safe timestamp.
     */
    public HybridTimestamp safeTimestamp() {
        return safeTimeClock.now();
    }

    /**
     * Sync safe time for this replica.
     *
     * @return Safe timestamp.
     */
    public HybridTimestamp syncSafeTimestamp(HybridTimestamp timestamp) {
        return safeTimeClock.sync(timestamp);
    }

    private void safeTimeSync() {
        // TODO HLC
        listener.invoke(new ReplicaMessagesFactory().safeTimeSyncRequest().safeTimestamp(safeTimestamp()).build());
    }
}
