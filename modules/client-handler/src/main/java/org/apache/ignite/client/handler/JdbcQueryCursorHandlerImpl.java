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

package org.apache.ignite.client.handler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFetchQueryResultsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryCursorHandlerImpl extends JdbcHandlerBase implements JdbcQueryCursorHandler {
    /**
     * Constructor.
     *
     * @param resources Client resources.
     */
    JdbcQueryCursorHandlerImpl(ClientResourceRegistry resources, Executor throttledLoggerExecutor) {
        super(resources, throttledLoggerExecutor);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryFetchResult> fetchAsync(JdbcFetchQueryResultsRequest req) {
        AsyncSqlCursor<InternalSqlRow> asyncSqlCursor;
        try {
            asyncSqlCursor = resources.get(req.cursorId()).get(AsyncSqlCursor.class);
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        if (req.fetchSize() <= 0) {
            return CompletableFuture.completedFuture(new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Invalid fetch size [fetchSize=" + req.fetchSize() + ']'));
        }

        return asyncSqlCursor.requestNextAsync(req.fetchSize()).handle((batch, t) -> {
            if (t != null) {
                StringWriter sw = getWriterWithStackTrace(t);

                return new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Failed to fetch query results [curId=" + req.cursorId() + "]. Error message: " + sw);
            }

            List<ByteBuffer> rows = new ArrayList<>(batch.items().size());
            for (InternalSqlRow item : batch.items()) {
                rows.add(item.asBinaryTuple().byteBuffer());
            }

            return new JdbcQueryFetchResult(rows, !batch.hasMore());
        }).toCompletableFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQuerySingleResult> getMoreResultsAsync(JdbcFetchQueryResultsRequest req) {
        AsyncSqlCursor<InternalSqlRow> asyncSqlCursor;
        try {
            asyncSqlCursor = resources.remove(req.cursorId()).get(AsyncSqlCursor.class);
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcQuerySingleResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        CompletableFuture<Void> cursorCloseFuture = asyncSqlCursor.closeAsync();

        if (!asyncSqlCursor.hasNextResult()) {
            // driver should check presence of next result set on client side and avoid unnecessary calls
            return CompletableFuture.completedFuture(new JdbcQuerySingleResult(Response.STATUS_FAILED,
                    "Cursor doesn't have next result"));
        }

        return cursorCloseFuture.thenCompose(c -> asyncSqlCursor.nextResult())
                .thenCompose(cur -> createJdbcResult(cur, req.fetchSize()))
                .exceptionally(t -> {
                    iterateThroughResultsAndCloseThem(asyncSqlCursor);

                    String msgPrefix = "Failed to fetch query results [curId=" + req.cursorId() + "].";

                    return createErrorResult(msgPrefix, t, msgPrefix + " Error message: ");
                });
    }

    private static void iterateThroughResultsAndCloseThem(AsyncSqlCursor<InternalSqlRow> cursor) {
        Function<AsyncSqlCursor<InternalSqlRow>, CompletableFuture<AsyncSqlCursor<InternalSqlRow>>> traverser = new Function<>() {
            @Override
            public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> apply(AsyncSqlCursor<InternalSqlRow> cur) {
                return cur.closeAsync()
                        .thenCompose(none -> {
                            if (cur.hasNextResult()) {
                                return cur.nextResult().thenCompose(this);
                            } else {
                                return CompletableFuture.completedFuture(cur);
                            }
                        });
            }
        };

        CompletableFuture.completedFuture(cursor).thenCompose(traverser);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryCloseResult> closeAsync(JdbcQueryCloseRequest req) {
        AsyncSqlCursor<List<Object>> asyncSqlCursor;
        try {
            if (req.removeFromResources()) {
                asyncSqlCursor = resources.remove(req.cursorId()).get(AsyncSqlCursor.class);
            } else {
                asyncSqlCursor = resources.get(req.cursorId()).get(AsyncSqlCursor.class);
            }
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcQueryCloseResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        return asyncSqlCursor.closeAsync().handle((none, t) -> {
            if (t != null) {
                StringWriter sw = getWriterWithStackTrace(t);

                return new JdbcQueryCloseResult(Response.STATUS_FAILED,
                        "Failed to close SQL query cursor [curId=" + req.cursorId() + "]. Error message: " + sw);
            }

            return new JdbcQueryCloseResult();
        });
    }

    /**
     * Serializes the stack trace of given exception for further sending to the client.
     *
     * @param t Throwable.
     * @return StringWriter filled with exception.
     */
    private StringWriter getWriterWithStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        t.printStackTrace(pw);
        return sw;
    }
}
