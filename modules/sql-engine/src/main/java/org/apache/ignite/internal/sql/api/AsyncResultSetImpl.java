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

package org.apache.ignite.internal.sql.api;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Asynchronous result set implementation.
 */
public class AsyncResultSetImpl implements AsyncResultSet {
    private static final CompletableFuture<? extends AsyncResultSet> HAS_NO_MORE_PAGE_FUTURE =
            CompletableFuture.failedFuture(new IgniteSqlException("There are no more pages."));

    private final AsyncSqlCursor<List<Object>> cur;

    private final BatchedResult<List<Object>> batchPage;

    private final Page page;

    private final int pageSize;

    private final Runnable closeRun;

    private final Object mux = new Object();

    private volatile CompletionStage<? extends AsyncResultSet> next;

    /**
     * Constructor.
     *
     * @param cur Asynchronous query cursor.
     */
    public AsyncResultSetImpl(AsyncSqlCursor<List<Object>> cur, BatchedResult<List<Object>> page, int pageSize, Runnable closeRun) {
        this.cur = cur;
        this.batchPage = page;
        this.pageSize = pageSize;
        this.closeRun = closeRun;
        this.page = new Page();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ResultSetMetadata metadata() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasRowSet() {
        return cur.queryType() == SqlQueryType.QUERY || cur.queryType() == SqlQueryType.EXPLAIN;
    }

    /** {@inheritDoc} */
    @Override
    public long affectedRows() {
        if (hasRowSet() || cur.queryType() == SqlQueryType.DDL) {
            return -1;
        }

        assert batchPage.items().size() == 1
                && batchPage.items().get(0).size() == 1
                && batchPage.items().get(0).get(0) instanceof Long
                && !batchPage.hasMore() : "Invalid DML result: " + batchPage;

        return (long) batchPage.items().get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasApplied() {
        if (hasRowSet() || cur.queryType() == SqlQueryType.DML) {
            return false;
        }

        assert batchPage.items().size() == 1
                && batchPage.items().get(0).size() == 1
                && batchPage.items().get(0).get(0) instanceof Boolean
                && !batchPage.hasMore() : "Invalid DDL result: " + batchPage;

        return (boolean) batchPage.items().get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<SqlRow> currentPage() {
        if (!hasRowSet()) {
            throw new NoRowSetExpectedException("Query hasn't result set: [type=" + cur.queryType() + ']');
        }

        return page;
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<? extends AsyncResultSet> fetchNextPage() {
        if (next == null) {
            synchronized (mux) {
                if (next == null) {
                    if (!hasMorePages()) {
                        next = HAS_NO_MORE_PAGE_FUTURE;
                    } else {
                        next = cur.requestNextAsync(pageSize)
                                .thenApply(batchRes -> new AsyncResultSetImpl(cur, batchRes, pageSize, closeRun));
                    }
                }

            }
        }

        return next;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return batchPage.hasMore();
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> closeAsync() {
        return cur.closeAsync().thenAccept((v) -> closeRun.run());
    }

    private class Page implements Iterable<SqlRow> {
        /** {@inheritDoc} */
        @NotNull
        @Override
        public Iterator<SqlRow> iterator() {
            return new IteratorImpl(batchPage.items().iterator());
        }
    }

    private class IteratorImpl implements Iterator<SqlRow> {
        private final Iterator<List<Object>> it;

        IteratorImpl(Iterator<List<Object>> it) {
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public SqlRow next() {
            return new SqlRowImpl(it.next());
        }
    }

    private class SqlRowImpl implements SqlRow {
        private final List<Object> row;

        SqlRowImpl(List<Object> row) {
            this.row = row;
        }

        /** {@inheritDoc} */
        @Override
        public int columnCount() {
            return cur.metadata().fields().size();
        }

        /** {@inheritDoc} */
        @Override
        public String columnName(int columnIndex) {
            return cur.metadata().fields().get(columnIndex).name();
        }

        /** {@inheritDoc} */
        @Override
        public int columnIndex(@NotNull String columnName) {
            return cur.metadata().fields().stream()
                    .filter(fld -> fld.name().equals(columnName))
                    .findFirst()
                    .get()
                    .order();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override
        public <T> T valueOrDefault(@NotNull String columnName, T defaultValue) {
            T ret = (T) row.get(columnIndex(columnName));

            return ret != null ? ret : defaultValue;
        }

        /** {@inheritDoc} */
        @Override
        public Tuple set(@NotNull String columnName, Object value) {
            throw new UnsupportedOperationException("Operation not supported.");
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override
        public <T> T value(@NotNull String columnName) throws IllegalArgumentException {
            return (T) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override
        public <T> T value(int columnIndex) {
            return (T) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public BinaryObject binaryObjectValue(@NotNull String columnName) {
            return (BinaryObject) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public BinaryObject binaryObjectValue(int columnIndex) {
            return (BinaryObject) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public byte byteValue(@NotNull String columnName) {
            return (byte) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public byte byteValue(int columnIndex) {
            return (byte) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public short shortValue(@NotNull String columnName) {
            return (short) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public short shortValue(int columnIndex) {
            return (short) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public int intValue(@NotNull String columnName) {
            return (int) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public int intValue(int columnIndex) {
            return (int) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public long longValue(@NotNull String columnName) {
            return (long) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public long longValue(int columnIndex) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override
        public float floatValue(@NotNull String columnName) {
            return (float) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public float floatValue(int columnIndex) {
            return (float) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public double doubleValue(@NotNull String columnName) {
            return (double) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public double doubleValue(int columnIndex) {
            return (double) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public String stringValue(@NotNull String columnName) {
            return (String) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public String stringValue(int columnIndex) {
            return (String) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public UUID uuidValue(@NotNull String columnName) {
            return (UUID) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public UUID uuidValue(int columnIndex) {
            return (UUID) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public BitSet bitmaskValue(@NotNull String columnName) {
            return (BitSet) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public BitSet bitmaskValue(int columnIndex) {
            return (BitSet) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public LocalDate dateValue(String columnName) {
            return (LocalDate) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public LocalDate dateValue(int columnIndex) {
            return (LocalDate) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public LocalTime timeValue(String columnName) {
            return (LocalTime) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public LocalTime timeValue(int columnIndex) {
            return (LocalTime) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public LocalDateTime datetimeValue(String columnName) {
            return (LocalDateTime) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public LocalDateTime datetimeValue(int columnIndex) {
            return (LocalDateTime) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public Instant timestampValue(String columnName) {
            return (Instant) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public Instant timestampValue(int columnIndex) {
            return (Instant) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @NotNull
        @Override
        public Iterator<Object> iterator() {
            return row.iterator();
        }

        @Override
        public ResultSetMetadata metadata() {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
    }
}
