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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.sql.SqlQueryApi;
import org.apache.ignite.internal.rest.api.sql.SqlQueryInfo;
import org.apache.ignite.internal.rest.sql.exception.SqlQueryNotFoundException;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.jetbrains.annotations.Nullable;

/**
 * REST endpoint allows to manage sql queries.
 */
@Controller("/management/v1/sql")
public class SqlQueryController implements SqlQueryApi, ResourceHolder {

    private IgniteSql igniteSql;

    private KillCommandHandler killCommandHandler;

    public SqlQueryController(IgniteSql igniteSql, KillCommandHandler killCommandHandler) {
        this.igniteSql = igniteSql;
        this.killCommandHandler = killCommandHandler;
    }

    @Override
    public CompletableFuture<Collection<SqlQueryInfo>> queries() {
        return completedFuture(sqlQueryInfos());
    }

    @Override
    public CompletableFuture<SqlQueryInfo> query(UUID queryId) {
        return completedFuture(sqlQueryInfos(uuid -> uuid.equals(queryId))).thenApply(queryInfo -> {
            if (queryInfo.isEmpty()) {
                throw new SqlQueryNotFoundException(queryId.toString());
            } else {
                return queryInfo.get(0);
            }
        });
    }

    @Override
    public CompletableFuture<Void> cancelQuery(UUID queryId) {
        return killCommandHandler.handler(CancellableOperationType.QUERY).cancelAsync(queryId.toString())
                .thenCompose(result -> handleOperationResult(queryId, result));
    }

    private static CompletableFuture<Void> handleOperationResult(UUID queryId, @Nullable Boolean result) {
        if (result != null && !result) {
            return failedFuture(new SqlQueryNotFoundException(queryId.toString()));
        } else {
            return nullCompletedFuture();
        }
    }

    @Override
    public void cleanResources() {
        igniteSql = null;
        killCommandHandler = null;
    }

    private List<SqlQueryInfo> sqlQueryInfos() {
        return sqlQueryInfos(null);
    }

    private List<SqlQueryInfo> sqlQueryInfos(Predicate<UUID> predicate) {
        String sql = "SELECT * FROM SYSTEM.SQL_QUERIES ORDER BY START_TIME";
        Statement sqlQueryStmt = igniteSql.createStatement(sql);
        List<SqlQueryInfo> sqlQueryInfos = new ArrayList<>();
        try (ResultSet<SqlRow> resultSet = igniteSql.execute(null, sqlQueryStmt)) {
            while (resultSet.hasNext()) {
                SqlRow row = resultSet.next();
                // Skip original query to SYSTEM.SQL_QUERIES
                if (row.stringValue("SQL").equals(sql)) {
                    continue;
                }
                // Filter query by id if needed
                if (predicate != null && !predicate.test(UUID.fromString(row.stringValue("ID")))) {
                    continue;
                }
                sqlQueryInfos.add(new SqlQueryInfo(
                        UUID.fromString(row.stringValue("ID")),
                        row.stringValue("INITIATOR_NODE"),
                        row.stringValue("PHASE"),
                        row.stringValue("TYPE"),
                        row.stringValue("SCHEMA"),
                        row.stringValue("SQL"),
                        row.timestampValue("START_TIME")));
            }
        }
        return sqlQueryInfos;
    }
}
