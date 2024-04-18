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

import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFetchQueryResultsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;

/**
 * Jdbc QUERY cursor operations handler interface.
 */
public interface JdbcQueryCursorHandler {
    /**
     * {@link JdbcFetchQueryResultsRequest} command handler.
     *
     * @param req Fetch query request.
     * @return Result future.
     */
    CompletableFuture<JdbcQueryFetchResult> fetchAsync(JdbcFetchQueryResultsRequest req);

    /**
     * {@link Statement#getMoreResults()} command implementor.
     *
     * @param req Results request.
     * @return Result future.
     */
    CompletableFuture<JdbcQuerySingleResult> getMoreResultsAsync(JdbcFetchQueryResultsRequest req);

    /**
     * {@link JdbcQueryCloseRequest} command handler.
     *
     * @param req Close query request.
     * @return Result future.
     */
    CompletableFuture<JdbcQueryCloseResult> closeAsync(JdbcQueryCloseRequest req);

    /**
     * {@link JdbcQueryMetadataRequest} command handler.
     *
     * @param req Jdbc query metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req);
}
