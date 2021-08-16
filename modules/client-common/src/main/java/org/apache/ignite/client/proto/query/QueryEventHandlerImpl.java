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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcBatchExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryCloseResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryExecuteResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchRequest;
import org.apache.ignite.client.proto.query.event.JdbcQueryFetchResult;
import org.apache.ignite.client.proto.query.event.JdbcQuerySingleResult;
import org.apache.ignite.client.proto.query.event.JdbcResponse;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.util.Cursor;

import static org.apache.ignite.client.proto.query.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;

/**
 * Jdbc query event handler implementation.
 */
public class QueryEventHandlerImpl implements JdbcQueryEventHandler {
    /** Current JDBC cursors. */
    private final ConcurrentHashMap<Long, SqlCursor<List<?>>> openCursors = new ConcurrentHashMap<>();

    /** Cursor Id generator. */
    private final AtomicLong CURSOR_ID_GENERATOR = new AtomicLong();

    /** Sql query processor. */
    private final QueryProcessor processor;

    /**
     * Constructor.
     *
     * @param processor Processor.
     */
    public QueryEventHandlerImpl(QueryProcessor processor) {
        this.processor = processor;
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryExecuteResult query(JdbcQueryExecuteRequest req) {
        if (req.pageSize() <= 0)
            return new JdbcQueryExecuteResult(JdbcResponse.STATUS_FAILED,
                "Invalid fetch size : [fetchSize=" + req.pageSize() + ']');

        var cursors = processor.query(req.schemaName(), req.sqlQuery(), req.arguments());

        if (cursors.isEmpty())
            return new JdbcQueryExecuteResult(JdbcResponse.STATUS_FAILED,
                "At least one cursor is expected for query " + req.sqlQuery());

        List<JdbcQuerySingleResult> results = new ArrayList<>();

        try {
            for (SqlCursor<List<?>> cur : cursors) {
                JdbcQuerySingleResult res = createJdbcResult(cur, req);
                results.add(res);
            }
        } catch (Exception ex) {
            return new JdbcQueryExecuteResult(JdbcResponse.STATUS_FAILED,
                "Failed to fetch results for query " + req.sqlQuery() + ". Error message: " + ex.getMessage());
        }

        return new JdbcQueryExecuteResult(results);
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryFetchResult fetch(JdbcQueryFetchRequest req) {
        Cursor<List<?>> cur = openCursors.get(req.cursorId());

        if (cur == null)
            return new JdbcQueryFetchResult(JdbcResponse.STATUS_FAILED,
                "Failed to find query cursor with ID: " + req.cursorId());

        if (req.pageSize() <= 0)
            return new JdbcQueryFetchResult(JdbcResponse.STATUS_FAILED,
                "Invalid fetch size : [fetchSize=" + req.pageSize() + ']');

        List<List<Object>> fetch;
        boolean hasNext;

        try {
            fetch = fetchNext(req.pageSize(), cur);
            hasNext = cur.hasNext();
        } catch (Exception ex) {
            return new JdbcQueryFetchResult(JdbcResponse.STATUS_FAILED,
                "Failed to fetch results for cursor id " + req.cursorId() + ". Error message: " + ex.getMessage());
        }

        return new JdbcQueryFetchResult(fetch, hasNext);
    }

    /** {@inheritDoc} */
    @Override public JdbcBatchExecuteResult batch(JdbcBatchExecuteRequest req) {
        return new JdbcBatchExecuteResult(UNSUPPORTED_OPERATION,
            "ExecuteBatch operation is not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryCloseResult close(JdbcQueryCloseRequest req) {
        Cursor<List<?>> cur = openCursors.remove(req.cursorId());

        if (cur == null)
            return new JdbcQueryCloseResult(JdbcResponse.STATUS_FAILED,
                "Failed to find query cursor with ID: " + req.cursorId());

        try {
            cur.close();
        }
        catch (Exception ex) {
            return new JdbcQueryCloseResult(JdbcResponse.STATUS_FAILED,
                "Failed to close SQL query [curId=" + req.cursorId() + "]. Error message: " + ex.getMessage());
        }

        return new JdbcQueryCloseResult();
    }

    /**
     * Creates jdbc result for the cursor.
     *
     * @param cur Sql cursor for query.
     * @param req Execution request.
     * @return JdbcQuerySingleResult filled with first batch of data.
     */
    private JdbcQuerySingleResult createJdbcResult(SqlCursor<List<?>> cur, JdbcQueryExecuteRequest req) {
        long cursorId = CURSOR_ID_GENERATOR.getAndIncrement();

        openCursors.put(cursorId, cur);

        List<List<Object>> fetch = fetchNext(req.pageSize(), cur);
        boolean hasNext = cur.hasNext();

        switch (cur.getQueryType()) {
            case QUERY:
                return new JdbcQuerySingleResult(cursorId, fetch, !hasNext);
            case DML:
            case DDL: {
                if (!validateDmlResult(fetch, hasNext))
                    return new JdbcQuerySingleResult(JdbcResponse.STATUS_FAILED,
                        "Unexpected result for DML query [" + req.sqlQuery() + "].");

                return new JdbcQuerySingleResult(cursorId, (Long)fetch.get(0).get(0));
            }
            case FRAGMENT:
            case EXPLAIN:
            default:
                return new JdbcQuerySingleResult(UNSUPPORTED_OPERATION,
                    "Query type [" + cur.getQueryType() + "] is not supported yet.");
        }
    }

    /**
     * Validate dml result. Check if it stores only one value of Long type.
     *
     * @param fetch Fetched data from cursor.
     * @param next HasNext flag.
     * @return Boolean value indicates if data is valid or not.
     */
    private boolean validateDmlResult(List<List<Object>> fetch, boolean next) {
        if (next)
            return false;

        if (fetch.size() != 1)
            return false;

        if (fetch.get(0).size() != 1)
            return false;

        return fetch.get(0).get(0) instanceof Long;
    }

    /**
     * Fetch next batch of data.
     *
     * @param size Batch size.
     * @param cursor Sql cursor.
     * @return Array of given size with data.
     */
    private List<List<Object>> fetchNext(int size, Cursor<List<?>> cursor) {
        List<List<Object>> fetch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (cursor.hasNext())
                fetch.add((List<Object>)cursor.next());
        }
        return fetch;
    }
}
