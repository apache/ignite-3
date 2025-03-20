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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.partition.replicator.schemacompat.SchemaCompatibilityValidator;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.TransactionIds;
import org.jetbrains.annotations.Nullable;

// TODO sanpwc javadoc.
// TODO sanpwc rename TableAware
public class TableAwareReplicaRequestPreProcessor {
    private final ClockService clockService;

    private final SchemaCompatibilityValidator schemaCompatValidator;

    private final SchemaSyncService schemaSyncService;

    public TableAwareReplicaRequestPreProcessor(
            ClockService clockService,
            SchemaCompatibilityValidator schemaCompatValidator,
            SchemaSyncService schemaSyncService
    ) {
        this.clockService = clockService;
        this.schemaCompatValidator = schemaCompatValidator;
        this.schemaSyncService = schemaSyncService;
    }

    public CompletableFuture<Void> preProcessTableAwareRequest(
            ReplicaRequest request,
            ReplicaPrimacy replicaPrimacy,
            UUID senderId
    ) {
        assert request instanceof TableAware : "Request should be TableAware [request=" + request.getClass().getSimpleName() + ']';

        // TODO sanpwc not null. Change and add assert.
        @Nullable HybridTimestamp opTs = getOperationTimestamp(request);
        // TODO sanpwc add  assert message.
        assert opTs != null;

        @Nullable HybridTimestamp opTsIfDirectRo = (request instanceof ReadOnlyDirectReplicaRequest) ? opTs : null;
        @Nullable HybridTimestamp txTs = getTxStartTimestamp(request);
        if (txTs == null) {
            txTs = opTsIfDirectRo;
        }

        assert opTs == null || txTs == null || opTs.compareTo(txTs) >= 0 : "Tx started at " + txTs + ", but opTs precedes it: " + opTs
                + "; request " + request;

        // TODO sanpwc smart enable.
//        assert txTs != null && opTs.compareTo(txTs) >= 0 : "Invalid request timestamps";

        int tableId = ((TableAware) request).tableId();

        @Nullable HybridTimestamp finalTxTs = txTs;
        Runnable validateClo = () -> {
            schemaCompatValidator.failIfTableDoesNotExistAt(opTs, tableId);

            boolean hasSchemaVersion = request instanceof SchemaVersionAwareReplicaRequest;

            if (hasSchemaVersion) {
                SchemaVersionAwareReplicaRequest versionAwareRequest = (SchemaVersionAwareReplicaRequest) request;

                schemaCompatValidator.failIfRequestSchemaDiffersFromTxTs(
                        finalTxTs,
                        versionAwareRequest.schemaVersion(),
                        tableId
                );
            }
        };

        return schemaSyncService.waitForMetadataCompleteness(opTs).thenRun(validateClo);
    }


    // TODO sanpwc adjust javadoc. Add todo for 22522 in order to adjust javadoc when colocation will be disabled.
    /**
     * Returns the txn operation timestamp.
     *
     * <ul>
     *     <li>For a read/write in an RW transaction, it's 'now'</li>
     *     <li>For an RO read (with readTimestamp), it's readTimestamp (matches readTimestamp in the transaction)</li>
     *     <li>For a direct read in an RO implicit transaction, it's the timestamp chosen (as 'now') to process the request</li>
     * </ul>
     *
     * <p>For other requests, op timestamp is not applicable and the validation is skipped.
     *
     * @param request The request.
     * @return The timestamp or {@code null} if not a tx operation request.
     */
    private @Nullable HybridTimestamp getOperationTimestamp(ReplicaRequest request) {
        HybridTimestamp opStartTs;
        // TODO sanpwc add comment explaining why it's required to evaluate opStartTs for all transacions.
        if (request instanceof ReadOnlyReplicaRequest) {
            opStartTs = ((ReadOnlyReplicaRequest) request).readTimestamp();
        } else {
            opStartTs = clockService.current();;
        }

        return opStartTs;
    }

    /**
     * Returns timestamp of transaction start (for RW/timestamped RO requests) or @{code null} for other requests.
     *
     * @param request Replica request corresponding to the operation.
     */
    //TODO sanpwc rename
    private static @Nullable HybridTimestamp getTxStartTimestamp(ReplicaRequest request) {
        HybridTimestamp txStartTimestamp;

        if (request instanceof ReadWriteReplicaRequest) {
            txStartTimestamp = beginRwTxTs((ReadWriteReplicaRequest) request);
        } else if (request instanceof ReadOnlyReplicaRequest) {
            txStartTimestamp = ((ReadOnlyReplicaRequest) request).readTimestamp();
        } else {
            txStartTimestamp = null;
        }
        return txStartTimestamp;
    }

    /**
     * Extracts begin timestamp of a read-write transaction from a request.
     *
     * @param request Read-write replica request.
     */
    static HybridTimestamp beginRwTxTs(ReadWriteReplicaRequest request) {
        return TransactionIds.beginTimestamp(request.transactionId());
    }
}
