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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleContainer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.IgniteUtils;
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

    /** Returns partition awareness metadata from an underlying cursor. */
    public @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata() {
        return cursor.partitionAwarenessMetadata();
    }

    /** Returns query cursor. */
    public AsyncSqlCursor<InternalSqlRow> cursor() {
        return cursor;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasRowSet() {
        SqlQueryType queryType = cursor.queryType();
        return queryType.hasRowSet();
    }

    /** {@inheritDoc} */
    @Override
    public long affectedRows() {
        SqlQueryType queryType = cursor.queryType();
        if (!queryType.returnsAffectedRows()) {
            return -1;
        }

        assert curPage.items().get(0).get(0) instanceof Long : "Invalid DML result: " + curPage;

        return (long) curPage.items().get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasApplied() {
        SqlQueryType queryType = cursor.queryType();
        if (!queryType.supportsWasApplied()) {
            return false;
        }

        assert curPage.items().get(0).get(0) instanceof Boolean : "Invalid DDL/KILL result: " + curPage;

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

    static class SqlRowImpl implements SqlRow, BinaryTupleContainer {
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
            int columnIndex = columnIndex(columnName);

            if (columnIndex == -1) {
                return defaultValue;
            }

            return (T) row.get(columnIndex);
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
            return (boolean) getValueNotNull(columnName);
        }

        /** {@inheritDoc} */
        @Override
        public boolean booleanValue(int columnIndex) {
            return (boolean) getValueNotNull(columnIndex);
        }

        /** {@inheritDoc} */
        @Override
        public byte byteValue(String columnName) {
            Number number = getValueNotNull(columnName);

            return castToByte(number);
        }

        /** {@inheritDoc} */
        @Override
        public byte byteValue(int columnIndex) {
            Number number = getValueNotNull(columnIndex);

            return castToByte(number);
        }

        /** {@inheritDoc} */
        @Override
        public short shortValue(String columnName) {
            Number number = getValueNotNull(columnName);

            return castToShort(number);
        }

        /** {@inheritDoc} */
        @Override
        public short shortValue(int columnIndex) {
            Number number = getValueNotNull(columnIndex);

            return castToShort(number);
        }

        /** {@inheritDoc} */
        @Override
        public int intValue(String columnName) {
            Number number = getValueNotNull(columnName);

            return castToInt(number);
        }

        /** {@inheritDoc} */
        @Override
        public int intValue(int columnIndex) {
            Number number = getValueNotNull(columnIndex);

            return castToInt(number);
        }

        /** {@inheritDoc} */
        @Override
        public long longValue(String columnName) {
            Number number = getValueNotNull(columnName);

            return castToLong(number);
        }

        /** {@inheritDoc} */
        @Override
        public long longValue(int columnIndex) {
            Number number = getValueNotNull(columnIndex);

            return castToLong(number);
        }

        /** {@inheritDoc} */
        @Override
        public float floatValue(String columnName) {
            Number number = getValueNotNull(columnName);

            return castToFloat(number);
        }

        /** {@inheritDoc} */
        @Override
        public float floatValue(int columnIndex) {
            Number number = getValueNotNull(columnIndex);

            return castToFloat(number);
        }

        /** {@inheritDoc} */
        @Override
        public double doubleValue(String columnName) {
            Number number = getValueNotNull(columnName);

            return castToDouble(number);
        }

        /** {@inheritDoc} */
        @Override
        public double doubleValue(int columnIndex) {
            Number number = getValueNotNull(columnIndex);

            return castToDouble(number);
        }

        /** {@inheritDoc} */
        @Override
        public BigDecimal decimalValue(String columnName) {
            return (BigDecimal) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public BigDecimal decimalValue(int columnIndex) {
            return (BigDecimal) row.get(columnIndex);
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
        public byte[] bytesValue(String columnName) {
            return (byte[]) row.get(columnIndexChecked(columnName));
        }

        /** {@inheritDoc} */
        @Override
        public byte[] bytesValue(int columnIndex) {
            return (byte[]) row.get(columnIndex);
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
        public BinaryTupleReader binaryTuple() {
            return row.asBinaryTuple();
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.tupleToString(this);
        }

        private <T> T getValueNotNull(int columnIndex) {
            Object value = row.get(columnIndex);

            if (value == null) {
                throw new NullPointerException(format(IgniteUtils.NULL_TO_PRIMITIVE_ERROR_MESSAGE, columnIndex));
            }

            return (T) value;
        }

        private <T> T getValueNotNull(String columnName) {
            Object value = row.get(columnIndexChecked(columnName));

            if (value == null) {
                throw new NullPointerException(format(IgniteUtils.NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE, columnName));
            }

            return (T) value;
        }


        private static byte castToByte(Number number) {
            if (number instanceof Byte) {
                return (byte) number;
            }

            if (number instanceof Long || number instanceof Integer || number instanceof Short) {
                long longVal = number.longValue();
                byte byteVal = number.byteValue();

                if (longVal == byteVal) {
                    return byteVal;
                }

                throw new ArithmeticException("Byte value overflow: " + number);
            }

            return (byte) number;
        }

        private static short castToShort(Number number) {
            if (number instanceof Short) {
                return (short) number;
            }

            if (number instanceof Long || number instanceof Integer || number instanceof Byte) {
                long longVal = number.longValue();
                short shortVal = number.shortValue();

                if (longVal == shortVal) {
                    return shortVal;
                }

                throw new ArithmeticException("Short value overflow: " + number);
            }

            return (short) number;
        }

        private static int castToInt(Number number) {
            if (number instanceof Integer) {
                return (int) number;
            }

            if (number instanceof Long || number instanceof Short || number instanceof Byte) {
                long longVal = number.longValue();
                int intVal = number.intValue();

                if (longVal == intVal) {
                    return intVal;
                }

                throw new ArithmeticException("Int value overflow: " + number);
            }

            return (int) number;
        }

        private static long castToLong(Number number) {
            if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
                return number.longValue();
            }

            return (long) number;
        }

        private static float castToFloat(Number number) {
            if (number instanceof Float) {
                return (float) number;
            }

            if (number instanceof Double) {
                double doubleVal = number.doubleValue();
                float floatVal = number.floatValue();

                //noinspection FloatingPointEquality
                if (doubleVal == floatVal || Double.isNaN(doubleVal)) {
                    return floatVal;
                }

                throw new ArithmeticException("Float value overflow: " + number);
            }

            return (float) number;
        }

        private static double castToDouble(Number number) {
            if (number instanceof Double || number instanceof Float) {
                return number.doubleValue();
            }

            return (double) number;
        }
    }
}
