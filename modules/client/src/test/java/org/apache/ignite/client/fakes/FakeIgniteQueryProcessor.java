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

package org.apache.ignite.client.fakes;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Fake {@link QueryProcessor}.
 */
public class FakeIgniteQueryProcessor implements QueryProcessor {
    public static final String FAILED_SQL = "SELECT FAIL";

    private final String name;

    Consumer<String> dataAccessListener;
    String lastScript;

    public FakeIgniteQueryProcessor(String name) {
        this.name = name;
    }

    @Override
    public CompletableFuture<QueryMetadata> prepareSingleAsync(
            SqlProperties properties,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
            SqlProperties properties,
            HybridTimestampTracker observableTimeTracker,
            @Nullable InternalTransaction transaction,
            @Nullable CancellationToken cancellationToken,
            String qry,
            Object... params
    ) {
        if (dataAccessListener != null) {
            dataAccessListener.accept(name);
        }

        if (FAILED_SQL.equals(qry)) {
            return CompletableFuture.failedFuture(new SqlException(STMT_VALIDATION_ERR, "Query failed"));
        }

        if (Commons.isMultiStatementQueryAllowed(properties)) {
            var sb = new StringBuilder(qry);

            sb.append(", arguments: [");

            for (Object arg : params) {
                sb.append(arg).append(", ");
            }

            sb.append(']').append(", ")
                    .append("defaultSchema=").append(properties.defaultSchema()).append(", ")
                    .append("defaultQueryTimeout=").append(properties.queryTimeout());

            lastScript = sb.toString();
        }

        return CompletableFuture.completedFuture(new FakeCursor(qry, properties, this));
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    public void setDataAccessListener(Consumer<String> dataAccessListener) {
        this.dataAccessListener = dataAccessListener;
    }
}
