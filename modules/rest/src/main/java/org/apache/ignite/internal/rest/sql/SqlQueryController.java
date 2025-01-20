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

package org.apache.ignite.internal.rest.sql;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.sql.SqlQueryApi;
import org.apache.ignite.internal.rest.api.sql.SqlQueryInfo;
import org.apache.ignite.internal.rest.sql.exception.SqlQueryCancelException;
import org.apache.ignite.internal.rest.sql.exception.SqlQueryNotFoundException;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.jetbrains.annotations.Nullable;

/**
 * REST endpoint allows to manage sql queries.
 */
@Controller("/management/v1/sql")
public class SqlQueryController implements SqlQueryApi, ResourceHolder {

    private SqlQueryProcessor sqlQueryProcessor;

    public SqlQueryController(SqlQueryProcessor sqlQueryProcessor) {
        this.sqlQueryProcessor = sqlQueryProcessor;
    }

    @Override
    public CompletableFuture<Collection<SqlQueryInfo>> queries() {
        return completedFuture(sqlQueryProcessor.runningQueries().stream()
                .map(SqlQueryController::toSqlQuery)
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<SqlQueryInfo> query(UUID queryId) {
        return completedFuture(toSqlQuery(sqlQueryProcessor.runningQuery(queryId)));
    }

    @Override
    public CompletableFuture<Void> cancelQuery(UUID queryId) {
        return sqlQueryProcessor.cancelQuery(queryId)
                .thenCompose(result -> handleOperationResult(queryId, result));
    }

    private static CompletableFuture<Void> handleOperationResult(UUID queryId, @Nullable Boolean result) {
        if (result == null) {
            return failedFuture(new SqlQueryNotFoundException(queryId.toString()));
        } else if (!result) {
            return failedFuture(new SqlQueryCancelException(queryId.toString()));
        } else {
            return nullCompletedFuture();
        }
    }

    @Override
    public void cleanResources() {
        sqlQueryProcessor = null;
    }

    private static @Nullable SqlQueryInfo toSqlQuery(@Nullable QueryInfo queryInfo) {
        if (queryInfo == null) {
            return null;
        }
        return new SqlQueryInfo(
                queryInfo.id(),
                queryInfo.phase().toString(),
                queryInfo.queryType() != null ? queryInfo.queryType().toString() : null,
                queryInfo.schema(),
                queryInfo.sql(),
                queryInfo.startTime()
        );
    }
}
