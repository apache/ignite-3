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

package org.apache.ignite.internal.table.distributed.replicator.handlers;

import static java.lang.String.format;
import static org.apache.ignite.internal.table.distributed.replicator.RemoteResourceIds.cursorId;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Replicator.CURSOR_CLOSE_ERR;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.partition.replicator.ReplicaPrimacy;
import org.apache.ignite.internal.partition.replicator.network.replication.ScanCloseReplicaRequest;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.tx.impl.FullyQualifiedResourceId;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.lang.IgniteException;

/**
 * Handler for {@link ScanCloseReplicaRequest}.
 */
public class ScanCloseRequestHandler implements ReplicaRequestHandler<ScanCloseReplicaRequest> {
    private final RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry;

    private final ZonePartitionId replicationGroupId;

    /**
     * Constructor.
     *
     * @param remotelyTriggeredResourceRegistry Resource registry.
     * @param replicationGroupId Replication group id.
     */
    public ScanCloseRequestHandler(
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            ZonePartitionId replicationGroupId
    ) {
        this.remotelyTriggeredResourceRegistry = remotelyTriggeredResourceRegistry;
        this.replicationGroupId = replicationGroupId;
    }

    @Override
    public CompletableFuture<?> handle(ScanCloseReplicaRequest request, ReplicaPrimacy replicaPrimacy) {
        FullyQualifiedResourceId cursorId = cursorId(request.transactionId(), request.scanId());

        try {
            remotelyTriggeredResourceRegistry.close(cursorId);
        } catch (IgniteException e) {
            String message = format("Close cursor exception [replicaGrpId=%s, msg=%s]", replicationGroupId, e.getMessage());

            throw new ReplicationException(CURSOR_CLOSE_ERR, message, e);
        }

        return nullCompletedFuture();
    }
}
