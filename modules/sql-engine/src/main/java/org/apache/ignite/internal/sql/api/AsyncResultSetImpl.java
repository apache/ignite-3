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

package org.apache.ignite.internal.sql.api;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Asynchronous result set implementation.
 */
public class AsyncResultSetImpl<T> implements AsyncResultSet<T> {
    private final AsyncSqlCursor<InternalSqlRow> cursor;

    private volatile BatchedResult<InternalSqlRow> curPage;

    private final int pageSize;

    /**
     * Constructor.
     *
     * @param cursor Query cursor representing the result of execution.
     * @param page Current page.
     * @param pageSize Size of the page to fetch.
     */
    public AsyncResultSetImpl(
            AsyncSqlCursor<InternalSqlRow> cursor,
            BatchedResult<InternalSqlRow> page,
            int pageSize
    ) {
        this.cursor = cursor;
        this.curPage = page;
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ResultSetMetadata metadata() {
        return hasRowSet() ? cursor.metadata() : null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasRowSet() {
        return cursor.queryType() == SqlQueryType.QUERY || cursor.queryType() == SqlQueryType.EXPLAIN;
    }

    /** {@inheritDoc} */
    @Override
    public long affectedRows() {
        if (cursor.queryType() != SqlQueryType.DML) {
            return -1;
        }

        assert curPage.items().get(0).get(0) instanceof Long : "Invalid DML result: " + curPage;

        return (long) curPage.items().get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasApplied() {
        if (cursor.queryType() != SqlQueryType.DDL) {
            return false;
        }

        assert curPage.items().get(0).get(0) instanceof Boolean : "Invalid DDL result: " + curPage;

        return (boolean) curPage.items().get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<T> currentPage() {
        requireResultSet();

        Iterator<InternalSqlRow> it0 = curPage.items().iterator();
        ResultSetMetadata meta0 = cursor.metadata();

        // TODO: IGNITE-18695 map rows to objects when mapper is provided.
        return () -> new TransformingIterator<>(it0, (item) -> (T) new SqlRowImpl(item, meta0));
    }

    /** {@inheritDoc} */
    @Override
    public int currentPageSize() {
        requireResultSet();

        return curPage.items().size();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends AsyncResultSet<T>> fetchNextPage() {
        requireResultSet();

        return cursor.requestNextAsync(pageSize)
                .thenApply(page -> {
                    curPage = page;

                    if (!curPage.hasMore()) {
                        closeAsync();
                    }

                    return this;
                });
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return curPage.hasMore();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        return cursor.closeAsync();
    }

    private void requireResultSet() {
        if (!hasRowSet()) {
            throw new NoRowSetExpectedException();
        }
    }

    private static class SqlRowImpl implements SqlRow {
        private final InternalSqlRow row;

        private final ResultSetMetadata meta;

        SqlRowImpl(InternalSqlRow row, ResultSetMetadata meta) {
            this.row = row;
            this.meta = meta;
        }

        /** {@inheritDoc} */
        @Override
        public int columnCount() {
            return meta.columns().size();
        }

        /** {@inheritDoc} */
        @Override
        public String columnName(int columnIndex) {
            return meta.columns().get(columnIndex).name();
        }

        /** {@inheritDoc} */
        @Override
        public int columnIndex(String columnName) {
            return meta.indexOf(columnName);
        }

        private int columnIndexChecked(String columnName) {
            int idx = columnIndex(columnName);

            if (idx == -1) {
                throw new IllegalArgumentException("Column doesn't exist [name=" + columnName + ']');
            }

            return idx;
        }

        /** {@inheritDoc} */
        @Override
        public <T> T valueOrDefault(String columnName, T defaultValue) {
            T ret = (T) row.get(columnIndexChecked(columnName));

            return ret != null ? ret : defaultValue;
        }

        /** {@inheritDoc} */
        @Override
        public Tuple set(String columnName, Object value) {
            throw new UnsupportedOperationException("Operation not supported.");
        }

        /** {@inheritDoc} */
        @Override
        public <T> T value(String columnName) throws IllegalArgumentException {
            return (T) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public <T> T value(int columnIndex) {
            return (T) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public boolean booleanValue(String columnName) {
            return (boolean) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public boolean booleanValue(int columnIndex) {
            return (boolean) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public byte byteValue(String columnName) {
            return (byte) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public byte byteValue(int columnIndex) {
            return (byte) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public short shortValue(String columnName) {
            return (short) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public short shortValue(int columnIndex) {
            return (short) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public int intValue(String columnName) {
            return (int) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public int intValue(int columnIndex) {
            return (int) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public long longValue(String columnName) {
            return (long) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public long longValue(int columnIndex) {
            return (long) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public float floatValue(String columnName) {
            return (float) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public float floatValue(int columnIndex) {
            return (float) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public double doubleValue(String columnName) {
            return (double) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public double doubleValue(int columnIndex) {
            return (double) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public String stringValue(String columnName) {
            return (String) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public String stringValue(int columnIndex) {
            return (String) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public UUID uuidValue(String columnName) {
            return (UUID) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public UUID uuidValue(int columnIndex) {
            return (UUID) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public BitSet bitmaskValue(String columnName) {
            return (BitSet) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public BitSet bitmaskValue(int columnIndex) {
            return (BitSet) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public LocalDate dateValue(String columnName) {
            return (LocalDate) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public LocalDate dateValue(int columnIndex) {
            return (LocalDate) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public LocalTime timeValue(String columnName) {
            return (LocalTime) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public LocalTime timeValue(int columnIndex) {
            return (LocalTime) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public LocalDateTime datetimeValue(String columnName) {
            return (LocalDateTime) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public LocalDateTime datetimeValue(int columnIndex) {
            return (LocalDateTime) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public Instant timestampValue(String columnName) {
            return (Instant) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public Instant timestampValue(int columnIndex) {
            return (Instant) row.get(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public ResultSetMetadata metadata() {
            return meta;
        }

        @Override
        public String toString() {
            return "Row " + row;
        }
    }
}
