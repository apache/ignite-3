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
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.GetEstimatedSizeRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ScanCloseReplicaRequest;
import org.apache.ignite.internal.partition.replicator.schemacompat.SchemaCompatibilityValidator;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.message.TableWriteIntentSwitchReplicaRequest;
import org.jetbrains.annotations.Nullable;

/**
 * TableAware requests pre processor. Request processing logic that is common for all TableAware requests like schema sync processing goes
 * here.
 */
public class TableAwareReplicaRequestPreProcessor {
    private final ClockService clockService;

    private final SchemaCompatibilityValidator schemaCompatValidator;

    private final SchemaSyncService schemaSyncService;

    private final NodeProperties nodeProperties;

    /** Constructor. */
    public TableAwareReplicaRequestPreProcessor(
            ClockService clockService,
            SchemaCompatibilityValidator schemaCompatValidator,
            SchemaSyncService schemaSyncService,
            NodeProperties nodeProperties
    ) {
        this.clockService = clockService;
        this.schemaCompatValidator = schemaCompatValidator;
        this.schemaSyncService = schemaSyncService;
        this.nodeProperties = nodeProperties;
    }

    /**
     * Pre processes {@link TableAware} request. In other words perform general for all TableAware requests part of the logic like schema
     * awaiting.
     *
     * @param request Request to be processed.
     * @param replicaPrimacy Replica primacy information.
     * @param senderId Node sender id.
     * @return Future with the result of the request.
     */
    public CompletableFuture<Void> preProcessTableAwareRequest(
            ReplicaRequest request,
            ReplicaPrimacy replicaPrimacy,
            UUID senderId
    ) {
        assert request instanceof TableAware : "Request should be TableAware [request=" + request.getClass().getSimpleName() + ']';

        HybridTimestamp opTs = getOperationTimestamp(request);

        if (nodeProperties.colocationEnabled()) {
            assert opTs != null : "Table aware operation timestamp must not be null [request=" + request + ']';
        }

        @Nullable HybridTimestamp opTsIfDirectRo = (request instanceof ReadOnlyDirectReplicaRequest) ? opTs : null;
        @Nullable HybridTimestamp txTs = getTxStartTimestamp(request);
        if (txTs == null) {
            txTs = opTsIfDirectRo;
        }

        assert txTs == null || opTs.compareTo(txTs) >= 0 : "Tx started at " + txTs + ", but opTs precedes it: " + opTs
                + "; request " + request;

        assert txTs == null
                ? request instanceof GetEstimatedSizeRequest || request instanceof ScanCloseReplicaRequest
                || request instanceof BuildIndexReplicaRequest || request instanceof TableWriteIntentSwitchReplicaRequest
                : opTs.compareTo(txTs) >= 0 :
                "Invalid request timestamps [request=" + request + ']';

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

    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Adjust javadoc.
    /**
     * Returns the operation timestamp. In case of colocation:
     * <ul>
     *     <li>For an RO read (with readTimestamp), it's readTimestamp (matches readTimestamp in the transaction)</li>
     *     <li>For all other requests - clockService.current()</li>
     * </ul>
     * Otherwise:
     * <ul>
     *     <li>For a read/write in an RW transaction, it's 'now'</li>
     *     <li>For an RO read (with readTimestamp), it's readTimestamp (matches readTimestamp in the transaction)</li>
     *     <li>For a direct read in an RO implicit transaction, it's the timestamp chosen (as 'now') to process the request</li>
     * </ul>
     *
     * @param request The request.
     * @return The timestamp or {@code null} if not a tx operation request.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove @Nullable and make it private.
    public @Nullable HybridTimestamp getOperationTimestamp(ReplicaRequest request) {
        HybridTimestamp opStartTs;

        if (nodeProperties.colocationEnabled()) {
            if (request instanceof ReadOnlyReplicaRequest) {
                opStartTs = ((ReadOnlyReplicaRequest) request).readTimestamp();
            } else {
                // Timestamp is returned for all types of TableAware requests in order to enable schema sync mechanism that on it's turn
                // eliminates the race between table processor publishing and request processing. Otherwise NPE may be thrown on retrieving
                // table processor by tableId from ZonePartitionReplicaListener.replicas.
                opStartTs = clockService.current();
            }
        } else {
            if (request instanceof ReadWriteReplicaRequest) {
                opStartTs = clockService.current();
            } else if (request instanceof ReadOnlyReplicaRequest) {
                opStartTs = ((ReadOnlyReplicaRequest) request).readTimestamp();
            } else if (request instanceof ReadOnlyDirectReplicaRequest) {
                opStartTs = clockService.current();
            } else {
                opStartTs = null;
            }
        }

        return opStartTs;
    }

    /**
     * Returns timestamp of transaction start (for RW/timestamped RO requests) or @{code null} for other requests.
     *
     * @param request Replica request corresponding to the operation.
     */
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
    private static HybridTimestamp beginRwTxTs(ReadWriteReplicaRequest request) {
        return TransactionIds.beginTimestamp(request.transactionId());
    }
}
