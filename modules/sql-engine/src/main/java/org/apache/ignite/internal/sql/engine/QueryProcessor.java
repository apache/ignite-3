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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;

/**
 * QueryProcessor interface.
 */
public interface QueryProcessor extends IgniteComponent {

    /**
     * Returns columns and parameters metadata for the given statement.
     * This method uses optional array of parameters to assist with type inference.
     *
     * @param properties User query properties. See {@link QueryProperty} for available properties.
     * @param transaction A transaction to use to resolve a schema.
     * @param qry Single statement SQL query.
     * @param params Query parameters.
     * @return Query metadata.
     *
     * @throws IgniteException in case of an error.
     * @see QueryProperty
     */
    CompletableFuture<QueryMetadata> prepareSingleAsync(SqlProperties properties,
            @Nullable InternalTransaction transaction,
            String qry, Object... params);

    /**
     * Execute the query with given schema name and parameters.
     *
     * @param properties Query properties. See {@link QueryProperty} for available properties.
     * @param transactions Transactions facade.
     * @param transaction A transaction to use for query execution. If null, an implicit transaction
     *      will be started by provided transactions facade.
     * @param qry SQL query.
     * @param params Query parameters.
     * @return Sql cursor.
     *
     * @throws IgniteException in case of an error.
     * @see QueryProperty
     */
    CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
            SqlProperties properties,
            IgniteTransactions transactions,
            @Nullable InternalTransaction transaction,
            String qry,
            Object... params
    );
}
