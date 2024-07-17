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

package org.apache.ignite.internal.tx.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.tx.impl.TxManagerImpl.TransactionFailureHandler;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Sends requests to switch write intents (to normal value for a commit and remove for an abort).
 */
public class WriteIntentSwitchProcessor {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(WriteIntentSwitchProcessor.class);

    /** Placement driver helper. */
    private final PlacementDriverHelper placementDriverHelper;

    private final TxMessageSender txMessageSender;

    /** Topology service. */
    private final TopologyService topologyService;

    /**
     * The constructor.
     *
     * @param placementDriverHelper Placement driver helper.
     * @param txMessageSender Transaction message creator.
     * @param topologyService Topology service.
     */
    public WriteIntentSwitchProcessor(
            PlacementDriverHelper placementDriverHelper,
            TxMessageSender txMessageSender,
            TopologyService topologyService
    ) {
        this.placementDriverHelper = placementDriverHelper;
        this.txMessageSender = txMessageSender;
        this.topologyService = topologyService;
    }

    /**
     * Run switch write intent on the provided node.
     */
    public CompletableFuture<ReplicaResponse> switchLocalWriteIntents(
            TablePartitionId tablePartitionId,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        String localNodeName = topologyService.localMember().name();

        return txMessageSender.switchWriteIntents(localNodeName, tablePartitionId, txId, commit, commitTimestamp);
    }

    /**
     * Run switch write intent on the primary node of the provided partition in a durable manner.
     */
    public CompletableFuture<ReplicaResponse> switchWriteIntentsWithRetry(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            TablePartitionId partitionId
    ) {
        return placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(partitionId)
                .thenCompose(leaseHolder ->
                        txMessageSender.switchWriteIntents(leaseHolder.getLeaseholder(), partitionId, txId, commit, commitTimestamp))
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ExceptionUtils.unwrapCause(ex);

                        if (TransactionFailureHandler.isRecoverable(cause)) {
                            LOG.info("Failed to switch write intents for Tx. The operation will be retried [txId={}].", txId, ex);

                            return switchWriteIntentsWithRetry(commit, commitTimestamp, txId, partitionId);
                        }

                        LOG.info("Failed to switch write intents for Tx [txId={}].", txId, ex);

                        return CompletableFuture.<ReplicaResponse>failedFuture(ex);
                    }

                    return CompletableFutures.<ReplicaResponse>nullCompletedFuture();
                })
                .thenCompose(Function.identity());
    }
}
