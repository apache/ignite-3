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

package org.apache.ignite.internal.jdbc.proto;

import java.sql.Connection;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcConnectResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFinishTxResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.Response;

/**
 * Jdbc client request handler.
 */
public interface JdbcQueryEventHandler {
    /**
     * Create connection context on a server and returns connection identity.
     *
     * @param timeZoneId Client time-zone ID.
     *
     * @return A future representing result of the operation.
     */
    CompletableFuture<JdbcConnectResult> connect(ZoneId timeZoneId);

    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param connectionId Identifier of the connection.
     * @param req Execute query request.
     * @return Result future.
     */
    CompletableFuture<? extends Response> queryAsync(long connectionId, JdbcQueryExecuteRequest req);

    /**
     * {@link JdbcBatchExecuteRequest} command handler.
     *
     * @param connectionId Identifier of the connection.
     * @param req Batch query request.
     * @return Result future.
     */
    CompletableFuture<JdbcBatchExecuteResult> batchAsync(long connectionId, JdbcBatchExecuteRequest req);

    /**
     * {@link JdbcBatchPreparedStmntRequest} command handler.
     *
     * @param connectionId The identifier of the connection.
     * @param req Batch query request.
     * @return Result future.
     */
    CompletableFuture<JdbcBatchExecuteResult> batchPrepStatementAsync(long connectionId, JdbcBatchPreparedStmntRequest req);

    /**
     * {@link JdbcMetaTablesRequest} command handler.
     *
     * @param req Jdbc tables metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req);

    /**
     * {@link JdbcMetaColumnsRequest} command handler.
     *
     * @param req Jdbc columns metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(JdbcMetaColumnsRequest req);

    /**
     * {@link JdbcMetaSchemasRequest} command handler.
     *
     * @param req Jdbc schemas metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(JdbcMetaSchemasRequest req);

    /**
     * {@link JdbcMetaPrimaryKeysRequest} command handler.
     *
     * @param req Jdbc primary keys metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(JdbcMetaPrimaryKeysRequest req);

    /**
     * Commit/rollback active transaction (if any) when {@link Connection#setAutoCommit(boolean)} autocommit} is disabled.
     *
     * @param connectionId An identifier of the connection on a server.
     * @param commit {@code True} to commit active transaction, {@code false} to rollback it.
     * @return Result future.
     */
    CompletableFuture<JdbcFinishTxResult> finishTxAsync(long connectionId, boolean commit);
}
