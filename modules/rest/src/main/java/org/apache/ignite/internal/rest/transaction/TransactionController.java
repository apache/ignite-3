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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.transaction.TransactionApi;
import org.apache.ignite.internal.rest.api.transaction.TransactionInfo;
import org.apache.ignite.internal.rest.transaction.exception.TransactionNotFoundException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;

/**
 * REST endpoint allows to manage transactions.
 */
@Controller("/management/v1/transaction")
public class TransactionController implements TransactionApi, ResourceHolder {

    private IgniteSql igniteSql;

    public TransactionController(IgniteSql igniteSql) {
        this.igniteSql = igniteSql;
    }

    @Override
    public CompletableFuture<Collection<TransactionInfo>> transactions() {
        return completedFuture(transactionInfos());
    }

    @Override
    public CompletableFuture<TransactionInfo> transaction(UUID transactionId) {
        return completedFuture(transactionInfos(uuid -> uuid.equals(transactionId))).thenApply(transactionInfos -> {
            if (transactionInfos.isEmpty()) {
                throw new TransactionNotFoundException(transactionId.toString());
            } else {
                return transactionInfos.get(0);
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
        igniteSql = null;
    }

    private List<TransactionInfo> transactionInfos() {
        return transactionInfos(null);
    }

    private List<TransactionInfo> transactionInfos(Predicate<UUID> predicate) {
        String sql = "SELECT * FROM SYSTEM.TRANSACTIONS ORDER BY START_TIME";
        Statement transactionStmt = igniteSql.createStatement(sql);
        List<TransactionInfo> transactionInfos = new ArrayList<>();
        try (ResultSet<SqlRow> resultSet = igniteSql.execute(null, transactionStmt)) {
            while (resultSet.hasNext()) {
                SqlRow row = resultSet.next();

                // Filter query by id if needed
                if (predicate != null && !predicate.test(UUID.fromString(row.stringValue("ID")))) {
                    continue;
                }
                transactionInfos.add(new TransactionInfo(
                        UUID.fromString(row.stringValue("ID")),
                        row.stringValue("COORDINATOR_NODE_ID"),
                        row.stringValue("STATE"),
                        row.stringValue("TYPE"),
                        row.stringValue("PRIORITY"),
                        row.timestampValue("START_TIME")));
            }
        }
        return transactionInfos;
    }
}
