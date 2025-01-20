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

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.transaction.Transaction;
import org.apache.ignite.internal.rest.api.transaction.TransactionApi;
import org.apache.ignite.internal.tx.TxManager;

/**
 * REST endpoint allows to manage transactions.
 */
@Controller("/management/v1/transaction")
public class TransactionController implements TransactionApi, ResourceHolder {

    public TransactionController(TxManager txManager) {
    }

    @Override
    public CompletableFuture<Collection<Transaction>> transactions() {
        return null;
    }

    @Override
    public CompletableFuture<Transaction> transaction(UUID transactionId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> cancelTransaction(UUID transactionId) {
        return null;
    }

    @Override
    public void cleanResources() {

    }
}
