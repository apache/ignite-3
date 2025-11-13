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

package org.apache.ignite.internal.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.internal.jdbc2.ClientSyncResultSet;
import org.apache.ignite.internal.jdbc2.JdbcResultSet;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Helper methods for creating a {@link ResultSet} using a list of objects.
 */
class JdbcDatabaseMetataUtils {
    /** List of support metadata column types. */
    private static final EnumSet<ColumnType> SUPPORTED_TYPES = EnumSet.of(
            ColumnType.NULL,
            ColumnType.BOOLEAN,
            ColumnType.INT8,
            ColumnType.INT16,
            ColumnType.INT32,
            ColumnType.INT64,
            ColumnType.STRING
    );

    /** Creates an empty {@link ResultSet} using the provided metadata. */
    static ResultSet createObjectListResultSet(List<ColumnMetadata> columnsMeta) {
        return createObjectListResultSet(List.of(), columnsMeta);
    }

    /** Creates a {@link ResultSet} using the provided list of objects and metadata. */
    static ResultSet createObjectListResultSet(List<List<Object>> rows, List<ColumnMetadata> columnsMeta) {
        ResultSetMetadata meta = new ResultSetMetadataImpl(columnsMeta);

        if (IgniteUtils.assertionsEnabled()) {
            for (ColumnMetadata columnMeta : meta.columns()) {
                assert SUPPORTED_TYPES.contains(columnMeta.type()) : "Unsupported column type: " + columnMeta.type();
            }
        }

        TransformingIterator<List<Object>, SqlRow> transformer =
                new TransformingIterator<>(rows.iterator(), ObjectListToSqlRowAdapter::new);

        return new JdbcResultSet(
                new IteratorBasedClientSyncResultSet(transformer, meta),
                null,
                null,
                false,
                0
        );
    }

    private static class IteratorBasedClientSyncResultSet implements ClientSyncResultSet {
        private final ResultSetMetadata metadata;
        private final Iterator<SqlRow> rowsIterator;

        IteratorBasedClientSyncResultSet(Iterator<SqlRow> rowsIterator, ResultSetMetadata metadata) {
            this.rowsIterator = rowsIterator;
            this.metadata = metadata;
        }

        @Override
        public ResultSetMetadata metadata() {
            return metadata;
        }

        @Override
        public boolean hasRowSet() {
            return true;
        }

        @Override
        public long affectedRows() {
            return -1;
        }

        @Override
        public boolean wasApplied() {
            return false;
        }

        @Override
        public boolean hasNextResultSet() {
            return false;
        }

        @Override
        public ClientSyncResultSet nextResultSet() {
            throw new NoSuchElementException("Query has no more results");
        }

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public boolean hasNext() {
            return rowsIterator.hasNext();
        }

        @Override
        public SqlRow next() {
            if (!rowsIterator.hasNext()) {
                throw new NoRowSetExpectedException();
            }

            return rowsIterator.next();
        }
    }

    private static class ObjectListToSqlRowAdapter implements SqlRow {
        private final List<Object> row;

        ObjectListToSqlRowAdapter(List<Object> row) {
            this.row = row;
        }

        @Override
        public <T> @Nullable T value(int columnIndex) {
            return (T) row.get(columnIndex);
        }

        @Override
        public <T> @Nullable T value(String columnName) throws IllegalArgumentException {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public ResultSetMetadata metadata() {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public int columnCount() {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public String columnName(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public int columnIndex(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public <T> @Nullable T valueOrDefault(String columnName, @Nullable T defaultValue) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public Tuple set(String columnName, @Nullable Object value) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public boolean booleanValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public boolean booleanValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public byte byteValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public byte byteValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public short shortValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public short shortValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public int intValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public int intValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public long longValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public long longValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public float floatValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public float floatValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public double doubleValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public double doubleValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public BigDecimal decimalValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public BigDecimal decimalValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public String stringValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public String stringValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public byte[] bytesValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public byte[] bytesValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public UUID uuidValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public UUID uuidValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public LocalDate dateValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public LocalDate dateValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public LocalTime timeValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public LocalTime timeValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public LocalDateTime datetimeValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public LocalDateTime datetimeValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public Instant timestampValue(String columnName) {
            throw new UnsupportedOperationException("This method should not be called.");
        }

        @Override
        public Instant timestampValue(int columnIndex) {
            throw new UnsupportedOperationException("This method should not be called.");
        }
    }
}
