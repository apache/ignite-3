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

import static org.apache.ignite.internal.sql.engine.QueryProperty.DEFAULT_SCHEMA;
import static org.apache.ignite.internal.sql.engine.QueryProperty.QUERY_TIMEOUT;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Fake {@link QueryProcessor}.
 */
public class FakeIgniteQueryProcessor implements QueryProcessor {
    public static final String FAILED_SQL = "SELECT FAIL";

    String lastScript;

    @Override
    public CompletableFuture<QueryMetadata> prepareSingleAsync(SqlProperties properties,
            @Nullable InternalTransaction transaction, String qry, Object... params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
            SqlProperties properties,
            HybridTimestampTracker observableTimeTracker,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    ) {
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
                    .append("defaultSchema=").append(properties.getOrDefault(DEFAULT_SCHEMA, "<not set>")).append(", ")
                    .append("defaultQueryTimeout=").append(properties.get(QUERY_TIMEOUT));

            lastScript = sb.toString();
        }

        return CompletableFuture.completedFuture(new FakeCursor(qry, properties, params, this));
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        return nullCompletedFuture();
    }
}
