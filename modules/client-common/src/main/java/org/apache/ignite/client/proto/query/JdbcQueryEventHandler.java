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

package org.apache.ignite.client.proto.query;

import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchResult;

/**
 * Jdbc client request handler.
 */
public interface JdbcQueryEventHandler {
    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result.
     */
    JdbcQueryExecuteResult query(JdbcQueryExecuteRequest req);

    /**
     * {@link JdbcQueryFetchRequest} command handler.
     *
     * @param req Fetch query request.
     * @return Result.
     */
    JdbcQueryFetchResult fetch(JdbcQueryFetchRequest req);

    /**
     * {@link JdbcBatchExecuteRequest} command handler.
     *
     * @param req Batch query request.
     * @return Result.
     */
    JdbcBatchExecuteResult batch(JdbcBatchExecuteRequest req);

    /**
     * {@link JdbcQueryCloseRequest} command handler.
     *
     * @param req Close query request.
     * @return Result.
     */
    JdbcQueryCloseResult close(JdbcQueryCloseRequest req);

    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result.
     */
    JdbcMetaTablesResult tablesMeta(JdbcMetaTablesRequest req);

    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result.
     */
    JdbcMetaColumnsResult columnsMeta(JdbcMetaColumnsRequest req);

    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result.
     */
    JdbcMetaSchemasResult schemasMeta(JdbcMetaSchemasRequest req);


    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result.
     */
    JdbcMetaPrimaryKeysResult primaryKeysMeta(JdbcMetaPrimaryKeysRequest req);
}
