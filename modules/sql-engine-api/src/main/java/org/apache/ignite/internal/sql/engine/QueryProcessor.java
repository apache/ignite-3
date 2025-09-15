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

package org.apache.ignite.internal.sql.engine;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * QueryProcessor interface.
 */
public interface QueryProcessor extends IgniteComponent {

    /**
     * Returns columns and parameters metadata for the given statement. This method uses optional array of parameters to assist with type
     * inference.
     *
     * @param properties User query properties.
     * @param transaction A transaction to use to resolve a schema.
     * @param qry Single statement SQL query.
     * @param params Query parameters.
     * @return Query metadata.
     * @throws IgniteException in case of an error.
     */
    CompletableFuture<QueryMetadata> prepareSingleAsync(
            SqlProperties properties,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    );

    /**
     * Execute the query with given schema name and parameters.
     *
     * @param observableTime Tracker of the latest time observed by client.
     * @param transaction A transaction to use for query execution. If null, an implicit transaction
     *      will be started by provided transactions facade.
     * @param cancellationToken Cancellation token or {@code null}.
     * @param qry SQL query.
     * @param params Query parameters.
     * @return Sql cursor.
     *
     * @throws IgniteException in case of an error.
     */
    CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
            SqlProperties properties,
            HybridTimestampTracker observableTime,
            @Nullable InternalTransaction transaction,
            @Nullable CancellationToken cancellationToken,
            String qry,
            Object... params
    );

    /**
     * Invalidates planner cache if {@code tableNames} is empty, otherwise invalidates only plans, which refers to the provided tables.
     *
     * @param tableNames Table names.
     * @return Operation completion future.
     */
    default CompletableFuture<Void> invalidatePlannerCache(Set<String> tableNames) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Planner implementation doesn't support cache."));
    }
}
