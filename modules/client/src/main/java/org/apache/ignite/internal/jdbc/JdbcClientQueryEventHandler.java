/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.jdbc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.BatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;
import org.apache.ignite.internal.jdbc.proto.event.QueryExecuteRequest;

/**
 * Jdbc client request handler.
 */
public interface JdbcClientQueryEventHandler {
    /**
     * {@link QueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result future.
     */
    CompletableFuture<List<JdbcClientQueryAsyncResult>> queryAsync(QueryExecuteRequest req);

    /**
     * {@link BatchExecuteRequest} command handler.
     *
     * @param req Batch query request.
     * @return Result future.
     */
    CompletableFuture<BatchExecuteResult> batchAsync(BatchExecuteRequest req);

    /**
     * {@link BatchPreparedStmntRequest} command handler.
     *
     * @param req Batch query request.
     * @return Result future.
     */
    CompletableFuture<BatchExecuteResult> batchPrepStatementAsync(BatchPreparedStmntRequest req);

    /**
     * {@link JdbcMetaTablesRequest} command handler.
     *
     * @param req Jdbc tables metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req);

    /**
     * Jdbc metadata column request command handler.
     *
     * @param schemaName Schema name pattern.
     * @param tblName Table name pattern.
     * @param colName Column name pattern.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(String schemaName, String tblName, String colName);

    /**
     * Jdbc metadata schemas command handler.
     *
     * @param schemaName Schema name pattern.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(String schemaName);

    /**
     * Jdbc metadata primary keys command handler.
     *
     * @param schemaName Schema name pattern.
     * @param tblName Table name pattern.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(String schemaName, String tblName);
}
