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
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Sql query cursor.
 *
 * @param <T> Type of elements.
 */
public interface AsyncSqlCursor<T> extends AsyncDataCursor<T> {
    /**
     * Returns query type.
     */
    SqlQueryType queryType();

    /**
     * Returns column metadata.
     */
    ResultSetMetadata metadata();

    /** Returns partition awareness metadata. */
    @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata();

    /**
     * Returns {@code true} if the current cursor is the result of a multi-statement query
     * and this statement is not the last one, {@code false} otherwise.
     */
    boolean hasNextResult();

    /**
     * Returns the future for the next statement of the query.
     *
     * @return Future that completes when the next statement completes.
     * @throws NoSuchElementException if the query has no more statements to execute.
     */
    CompletableFuture<AsyncSqlCursor<T>> nextResult();
}
