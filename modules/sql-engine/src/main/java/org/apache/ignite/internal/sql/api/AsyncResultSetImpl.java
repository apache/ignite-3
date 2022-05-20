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
 * Asynchronous result set provides methods for query results processing in asynchronous way.
 */
public class AsyncResultSetImpl implements AsyncResultSet {
    private final AsyncSqlCursor<List<Object>> cur;

    private final BatchedResult<List<Object>> page;

    private final int pageSize;

    /**
     * Constructor.
     *
     * @param cur Asynchronous query cursor.
     */
    public AsyncResultSetImpl(AsyncSqlCursor<List<Object>> cur, BatchedResult<List<Object>> page, int pageSize) {
        this.cur = cur;
        this.page = page;
        this.pageSize = pageSize;
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
    public int affectedRows() {
        if (hasRowSet() || cur.queryType() == SqlQueryType.DDL) {
            return -1;
        }

        assert page.items().size() == 1
                && page.items().get(0).size() == 1
                && page.items().get(0).get(0) instanceof Number
                && !page.hasMore() : "Invalid DML result: " + page;

        return (int) page.items().get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasApplied() {
        if (hasRowSet() || cur.queryType() == SqlQueryType.DML) {
            return false;
        }

        assert page.items().size() == 1
                && page.items().get(0).size() == 1
                && page.items().get(0).get(0) instanceof Boolean
                && !page.hasMore() : "Invalid DDL result: " + page;

        return (boolean) page.items().get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<SqlRow> currentPage() {
        if (!hasRowSet()) {
            throw new NoRowSetExpectedException("Query hasn't result set: [type=" + cur.queryType() + ']');
        }

        return new Page();
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<? extends AsyncResultSet> fetchNextPage() {
        return cur.requestNextAsync(pageSize)
                .thenApply(batchRes -> new AsyncResultSetImpl(cur, batchRes, pageSize));
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return page.hasMore();
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> closeAsync() {
        return null;
    }

    private class Page implements Iterable<SqlRow> {
        /** {@inheritDoc} */
        @NotNull
        @Override
        public Iterator<SqlRow> iterator() {
            return page.items().stream()
                    .map(RowTuple::new)
                    .map(SqlRow.class::cast)
                    .iterator();
        }
    }

    private class RowTuple implements SqlRow {
        private final List<Object> row;

        RowTuple(List<Object> row) {
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
        @Override
        public <T> T value(@NotNull String columnName) throws IllegalArgumentException {
            return (T) row.get(columnIndex(columnName));
        }

        /** {@inheritDoc} */
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
