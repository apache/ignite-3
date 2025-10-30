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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * FinalTransactionStateResolver implementation using retries. Makes sense when the coordinator of the transaction is known
 * to have left the topology.
 */
public class RetryingFinalTransactionStateResolver implements FinalTransactionStateResolver {
    private final TransactionStateResolver stateResolver;

    private final Executor delayedExecutor;

    /** Constructor. */
    public RetryingFinalTransactionStateResolver(TransactionStateResolver stateResolver, Executor executor) {
        this.stateResolver = stateResolver;
        delayedExecutor = CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS, executor);
    }

    @Override
    public CompletableFuture<TxState> resolveFinalTxState(UUID transactionId, ReplicationGroupId commitGroupId) {
        return stateResolver.resolveTxState(transactionId, commitGroupId, null, Long.MAX_VALUE, SECONDS)
                .thenCompose(txMeta -> {
                    if (txMeta != null && TxState.isFinalState(txMeta.txState())) {
                        return completedFuture(txMeta.txState());
                    }

                    return retryResolve(transactionId, commitGroupId);
                })
                .handle((res, ex) -> {
                    if (ex instanceof PrimaryReplicaAwaitTimeoutException) {
                        return retryResolve(transactionId, commitGroupId);
                    }

                    return CompletableFutures.completedOrFailedFuture(res, ex);
                })
                .thenCompose(identity());
    }

    private CompletableFuture<TxState> retryResolve(UUID transactionId, ReplicationGroupId commitGroupId) {
        return supplyAsync(() -> resolveFinalTxState(transactionId, commitGroupId), delayedExecutor)
                .thenCompose(identity());
    }
}
