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

package org.apache.ignite.internal.rest.transaction;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.transaction.TransactionApi;
import org.apache.ignite.internal.rest.api.transaction.TransactionInfo;
import org.apache.ignite.internal.rest.transaction.exception.TransactionNotFoundException;
import org.apache.ignite.internal.tx.views.TransactionViewDataProvider;
import org.apache.ignite.internal.tx.views.TxInfo;
import org.jetbrains.annotations.Nullable;

/**
 * REST endpoint allows to manage transactions.
 */
@Controller("/management/v1/transaction")
public class TransactionController implements TransactionApi, ResourceHolder {

    private TransactionViewDataProvider transactionViewDataProvider;

    public TransactionController(TransactionViewDataProvider transactionViewDataProvider) {
        this.transactionViewDataProvider = transactionViewDataProvider;
    }

    @Override
    public CompletableFuture<Collection<TransactionInfo>> transactions() {
        return completedFuture(StreamSupport.stream(transactionViewDataProvider.dataSource().spliterator(), false)
                .map(TransactionController::toTransaction)
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<TransactionInfo> transaction(UUID transactionId) {
        return completedFuture(StreamSupport.stream(transactionViewDataProvider.dataSource().spliterator(), false)
                .filter(tx -> tx.id().equals(transactionId))
                .findAny()
                .orElse(null))
                .thenApply(txInfo -> {
                    if (txInfo == null) {
                        throw new TransactionNotFoundException(transactionId.toString());
                    } else {
                        return toTransaction(txInfo);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> cancelTransaction(UUID transactionId) {
        // Waiting https://issues.apache.org/jira/browse/IGNITE-23488
        return nullCompletedFuture();
    }

    @Override
    public void cleanResources() {
        transactionViewDataProvider = null;
    }

    private static @Nullable TransactionInfo toTransaction(TxInfo txInfo) {
        if (txInfo == null) {
            return null;
        }
        return new TransactionInfo(
                UUID.fromString(txInfo.id()),
                txInfo.state(),
                txInfo.type(),
                txInfo.priority(),
                txInfo.startTime()
        );
    }
}
