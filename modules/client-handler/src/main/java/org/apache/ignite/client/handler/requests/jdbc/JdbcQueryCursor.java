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

package org.apache.ignite.client.handler.requests.jdbc;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Jdbc query cursor with the ability to limit the maximum number of rows returned.
 *
 * <p>The {@link JdbcQueryCursor#maxRows} parameter limits the amount of rows to be returned by the cursor.
 * Its value can either be a positive value or equal to zero, where zero means no limit.
 */
public class JdbcQueryCursor<T> implements AsyncSqlCursor<T> {
    /** Max rows. */
    private final long maxRows;

    /** Query result rows. */
    private final AsyncSqlCursor<T> cur;

    /** Number of fetched rows. */
    private final AtomicLong fetched = new AtomicLong();

    /**
     * Constructor.
     *
     * @param maxRows Max amount of rows cursor will return, or zero if unlimited.
     * @param cur Query cursor.
     */
    public JdbcQueryCursor(int maxRows, AsyncSqlCursor<T> cur) {
        this.maxRows = maxRows;
        this.cur = cur;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchedResult<T>> requestNextAsync(int rows) {
        long fetched0 = fetched.addAndGet(rows);

        assert cur != null : "non initialized cursor";

        return cur.requestNextAsync(rows).thenApply(batch -> {
            if (maxRows == 0 || fetched0 < maxRows) {
                return batch;
            }

            int remainCnt = (int) (maxRows - fetched0 + rows);

            List<T> remainItems = remainCnt < batch.items().size()
                    ? batch.items().subList(0, remainCnt)
                    : batch.items();

            return new BatchedResult<>(remainItems, false);
        });
    }

    /** {@inheritDoc} */
    @Override 
    public CompletableFuture<Void> closeAsync() {
        return cur.closeAsync();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> onClose() {
        return cur.onClose();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> onFirstPageReady() {
        return cur.onFirstPageReady();
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType queryType() {
        return cur.queryType();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return cur.metadata();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNextResult() {
        return cur.hasNextResult();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<T>> nextResult() {
        if (!hasNextResult()) {
            throw new NoSuchElementException("Query has no more results");
        }

        return cur.nextResult();
    }
}
