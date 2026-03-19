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
import static java.util.concurrent.CompletableFuture.failedFuture;

import io.micronaut.http.annotation.Controller;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.transaction.TransactionApi;
import org.apache.ignite.internal.rest.api.transaction.TransactionInfo;
import org.apache.ignite.internal.rest.transaction.exception.TransactionKillException;
import org.apache.ignite.internal.rest.transaction.exception.TransactionNotFoundException;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.KillHandlerRegistry;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * REST endpoint allows to manage transactions.
 */
@Controller("/management/v1/transaction")
public class TransactionController implements TransactionApi, ResourceHolder {

    private static final IgniteLogger LOG = Loggers.forClass(TransactionController.class);

    private IgniteSql igniteSql;

    private KillHandlerRegistry killHandlerRegistry;

    public TransactionController(IgniteSql igniteSql, KillHandlerRegistry killHandlerRegistry) {
        this.igniteSql = igniteSql;
        this.killHandlerRegistry = killHandlerRegistry;
    }

    @Override
    public CompletableFuture<Collection<TransactionInfo>> transactions() {
        return transactionInfos();
    }

    @Override
    public CompletableFuture<TransactionInfo> transaction(UUID transactionId) {
        return transactionInfos(transactionId).thenApply(transactionInfos -> {
            Iterator<TransactionInfo> iterator = transactionInfos.iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                throw new TransactionNotFoundException(transactionId.toString());
            }
        });
    }

    @Override
    public CompletableFuture<Void> killTransaction(UUID transactionId) {
        try {
            return killHandlerRegistry.handler(CancellableOperationType.TRANSACTION).cancelAsync(transactionId.toString())
                    .thenApply(result -> handleOperationResult(transactionId, result));
        } catch (Exception e) {
            LOG.error("Transaction {} can't be killed.", transactionId, e);
            return failedFuture(new TransactionKillException(transactionId.toString()));
        }
    }

    private static Void handleOperationResult(UUID transactionId, @Nullable Boolean result) {
        if (result != null && !result) {
            throw new TransactionNotFoundException(transactionId.toString());
        } else {
            return null;
        }
    }

    @Override
    public void cleanResources() {
        igniteSql = null;
        killHandlerRegistry = null;
    }

    private CompletableFuture<Collection<TransactionInfo>> transactionInfos() {
        return transactionInfos("SELECT * FROM SYSTEM.TRANSACTIONS ORDER BY START_TIME");
    }

    private CompletableFuture<Collection<TransactionInfo>> transactionInfos(UUID transactionId) {
        return transactionInfos("SELECT * FROM SYSTEM.TRANSACTIONS WHERE ID='" + transactionId.toString() + "'");
    }

    private CompletableFuture<Collection<TransactionInfo>> transactionInfos(String query) {
        Statement transactionStmt = igniteSql.createStatement(query);
        return igniteSql.executeAsync((Transaction) null, transactionStmt)
                .thenCompose(resultSet -> iterate(resultSet, new ArrayList<>()));
    }

    private static CompletableFuture<Collection<TransactionInfo>> iterate(AsyncResultSet<SqlRow> resultSet, List<TransactionInfo> result) {
        for (SqlRow row : resultSet.currentPage()) {
            result.add(convert(row));
        }
        if (resultSet.hasMorePages()) {
            return resultSet.fetchNextPage().thenCompose(nextPage -> iterate(nextPage, result));
        } else {
            return completedFuture(result);
        }
    }

    private static TransactionInfo convert(SqlRow row) {
        return new TransactionInfo(
                UUID.fromString(row.stringValue("ID")),
                row.stringValue("COORDINATOR_NODE_ID"),
                row.stringValue("STATE"),
                row.stringValue("TYPE"),
                row.stringValue("PRIORITY"),
                row.timestampValue("START_TIME"));
    }
}
