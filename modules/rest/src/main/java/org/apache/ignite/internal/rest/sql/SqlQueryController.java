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

import io.micronaut.http.annotation.Controller;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.sql.SqlQueryApi;
import org.apache.ignite.internal.rest.api.sql.SqlQueryInfo;
import org.apache.ignite.internal.rest.sql.exception.SqlQueryCancelException;
import org.apache.ignite.internal.rest.sql.exception.SqlQueryNotFoundException;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.KillHandlerRegistry;
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

    private static final IgniteLogger LOG = Loggers.forClass(SqlQueryController.class);

    private IgniteSql igniteSql;

    private KillHandlerRegistry killHandlerRegistry;

    public SqlQueryController(IgniteSql igniteSql, KillHandlerRegistry killHandlerRegistry) {
        this.igniteSql = igniteSql;
        this.killHandlerRegistry = killHandlerRegistry;
    }

    @Override
    public CompletableFuture<Collection<SqlQueryInfo>> queries() {
        return completedFuture(sqlQueryInfos());
    }

    @Override
    public CompletableFuture<SqlQueryInfo> query(UUID queryId) {
        return completedFuture(sqlQueryInfos(queryId)).thenApply(queryInfo -> {
            if (queryInfo.isEmpty()) {
                throw new SqlQueryNotFoundException(queryId.toString());
            } else {
                return queryInfo.get(0);
            }
        });
    }

    @Override
    public CompletableFuture<Void> cancelQuery(UUID queryId) {
        try {
            return killHandlerRegistry.handler(CancellableOperationType.QUERY).cancelAsync(queryId.toString())
                    .thenApply(result -> handleOperationResult(queryId, result));
        } catch (Exception e) {
            LOG.error("Sql query {} can't be canceled.", queryId, e);
            return failedFuture(new SqlQueryCancelException(queryId.toString()));
        }
    }

    private static Void handleOperationResult(UUID queryId, @Nullable Boolean result) {
        if (result != null && !result) {
            throw new SqlQueryNotFoundException(queryId.toString());
        } else {
            return null;
        }
    }

    @Override
    public void cleanResources() {
        igniteSql = null;
        killHandlerRegistry = null;
    }

    private List<SqlQueryInfo> sqlQueryInfos() {
        return sqlQueryInfos("SELECT * FROM SYSTEM.SQL_QUERIES ORDER BY START_TIME");
    }

    private List<SqlQueryInfo> sqlQueryInfos(UUID queryId) {
        return sqlQueryInfos("SELECT * FROM SYSTEM.SQL_QUERIES WHERE ID='" + queryId.toString() + "'");
    }

    private List<SqlQueryInfo> sqlQueryInfos(String query) {
        Statement sqlQueryStmt = igniteSql.createStatement(query);
        List<SqlQueryInfo> sqlQueryInfos = new ArrayList<>();
        try (ResultSet<SqlRow> resultSet = igniteSql.execute(null, sqlQueryStmt)) {
            while (resultSet.hasNext()) {
                SqlRow row = resultSet.next();
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
