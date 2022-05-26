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

package org.apache.ignite.client.handler.requests.jdbc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.SqlQueryType;

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
    public CompletionStage<BatchedResult<T>> requestNextAsync(int rows) {
        long fetched0 = fetched.addAndGet(rows);
        return cur.requestNextAsync(rows).thenApply(batch -> {
            if (maxRows == 0 || fetched0 < maxRows) {
                return batch;
            }

            if (fetched0 - rows < maxRows) {
                return new BatchedResult<>(batch.items()
                        .subList(0, (int) (maxRows - fetched0 + rows)), false);
            }

            return new BatchedResult<>(List.of(), false);
        });
    }

    /** {@inheritDoc} */
    @Override 
    public CompletableFuture<Void> closeAsync() {
        return cur.closeAsync();
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
}
