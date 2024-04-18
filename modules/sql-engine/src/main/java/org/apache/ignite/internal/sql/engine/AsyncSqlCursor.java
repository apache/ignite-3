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

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Sql query cursor.
 *
 * @param <T> Type of elements.
 */
public interface AsyncSqlCursor<T> extends AsyncCursor<T> {
    /**
     * Returns query type.
     */
    SqlQueryType queryType();

    /**
     * Returns column metadata.
     */
    ResultSetMetadata metadata();

    /**
     * Returns {@code true} if the current cursor is the result of a multi-statement query
     * and this statement is not the last one, {@code false} otherwise.
     */
    boolean hasNextResult();

    /**
     * Returns {@link CompletableFuture} that will be completed when cursor is closed.
     *
     * <p>The future will be completed with {@link null} if cursor was completed successfully,
     * or with an exception otherwise.
     *
     * @return A future representing result of operation.
     */
    CompletableFuture<Void> onClose();

    /**
     * Returns {@link CompletableFuture} that will be completed when first page is fetched and ready
     * to be returned to user.
     *
     * @return A future representing result of operation.
     */
    CompletableFuture<Void> onFirstPageReady();

    /**
     * Returns the future for the next statement of the query.
     *
     * @return Future that completes when the next statement completes.
     * @throws NoSuchElementException if the query has no more statements to execute.
     */
    CompletableFuture<AsyncSqlCursor<T>> nextResult();
}
