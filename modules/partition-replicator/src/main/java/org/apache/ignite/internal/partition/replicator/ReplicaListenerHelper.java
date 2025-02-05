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

package org.apache.ignite.internal.partition.replicator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.schema.SchemaSyncService;

/**
 * Contains code common for {@link ReplicaListener} implementations.
 *
 * <p>To be removed after PartitionReplicaListener is removed.
 */
public class ReplicaListenerHelper {
    private final SchemaSyncService schemaSyncService;
    private final CatalogService catalogService;
    private final RaftCommandRunner raftCommandRunner;
    private final ReplicationGroupId replicationGroupId;

    /** Constructor. */
    public ReplicaListenerHelper(
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            RaftCommandRunner raftCommandRunner,
            ReplicationGroupId replicationGroupId
    ) {
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
        this.raftCommandRunner = raftCommandRunner;
        this.replicationGroupId = replicationGroupId;
    }

    /**
     * Returns Catalog version corresponding to the given timestamp.
     *
     * @param ts Timestamp for which a Catalog version is to be obtained.
     */
    public CompletableFuture<Integer> reliableCatalogVersionFor(HybridTimestamp ts) {
        return schemaSyncService.waitForMetadataCompleteness(ts)
                .thenApply(unused -> catalogService.activeCatalogVersion(ts.longValue()));
    }

    /**
     * Executes a command and handles exceptions. A result future can be finished with exception by following rules:
     * <ul>
     *     <li>If RAFT command cannot finish due to timeout, the future finished with {@link ReplicationTimeoutException}.</li>
     *     <li>If RAFT command finish with a runtime exception, the exception is moved to the result future.</li>
     *     <li>If RAFT command finish with any other exception, the future finished with {@link ReplicationException}.
     *     The original exception is set as cause.</li>
     * </ul>
     *
     * @param cmd Raft command.
     * @return Raft future or raft decorated future with command that was processed.
     */
    public CompletableFuture<ResultWrapper<Object>> applyCmdWithExceptionHandling(Command cmd) {
        CompletableFuture<ResultWrapper<Object>> resultFuture = new CompletableFuture<>();

        raftCommandRunner.run(cmd).whenComplete((res, ex) -> {
            if (ex != null) {
                resultFuture.completeExceptionally(ex);
            } else {
                resultFuture.complete(new ResultWrapper<>(cmd, res));
            }
        });

        return resultFuture.exceptionally(throwable -> {
            if (throwable instanceof TimeoutException) {
                throw new ReplicationTimeoutException(replicationGroupId);
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new ReplicationException(replicationGroupId, throwable);
            }
        });
    }
}
