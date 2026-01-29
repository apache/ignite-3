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

import static org.apache.ignite.internal.tx.TxState.isFinalState;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.TxRecoveryEngine;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TransactionLogUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;

/**
 * Handler for processing {@link TxRecoveryMessage}s.
 */
public class TxRecoveryMessageHandler {
    private static final IgniteLogger LOG = Loggers.forClass(TxRecoveryMessageHandler.class);

    private final TxStatePartitionStorage txStatePartitionStorage;
    private final ZonePartitionId replicationGroupId;
    private final TxRecoveryEngine txRecoveryEngine;
    private final TxManager txManager;

    /** Constructor. */
    public TxRecoveryMessageHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            ZonePartitionId replicationGroupId,
            TxRecoveryEngine txRecoveryEngine,
            TxManager txManager
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.replicationGroupId = replicationGroupId;
        this.txRecoveryEngine = txRecoveryEngine;
        this.txManager = txManager;
    }

    /**
     * Processes transaction recovery request on a commit partition.
     *
     * @param request Tx recovery request.
     * @return The future is complete when the transaction state is finalized.
     */
    public CompletableFuture<Void> handle(TxRecoveryMessage request, UUID senderId) {
        UUID txId = request.txId();

        TxMeta txMeta = txStatePartitionStorage.get(txId);

        // Check whether a transaction has already been finished.
        if (txMeta != null && isFinalState(txMeta.txState())) {
            // Tx recovery message is processed on the commit partition.
            return txRecoveryEngine.runCleanupOnNode(replicationGroupId, txId, senderId);
        }

        LOG.info(
                "Orphan transaction has to be aborted [{}, meta={}].",
                TransactionLogUtils.formatTxInfo(txId, txManager, false),
                txMeta
        );

        return txRecoveryEngine.triggerTxRecovery(txId, senderId);
    }
}
