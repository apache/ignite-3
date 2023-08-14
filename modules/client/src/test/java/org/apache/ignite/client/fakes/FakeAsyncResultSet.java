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

package org.apache.ignite.client.fakes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Fake result set.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class FakeAsyncResultSet implements AsyncResultSet {
    private final Session session;

    private final Transaction transaction;

    private final Statement statement;

    private final Object[] arguments;

    private final List<SqlRow> rows;

    private final List<ColumnMetadata> columns;

    private final boolean hasMorePages;

    /**
     * Constructor.
     *
     * @param session Session.
     * @param transaction Transaction.
     * @param statement Statement.
     * @param arguments Arguments.
     */
    public FakeAsyncResultSet(Session session, Transaction transaction, Statement statement, Object[] arguments) {
        assert session != null;
        assert statement != null;

        this.session = session;
        this.transaction = transaction;
        this.statement = statement;
        this.arguments = arguments;

        hasMorePages = session.property("hasMorePages") != null;

        if ("SELECT PROPS".equals(statement.query())) {
            rows = new ArrayList<>();

            rows.add(getRow("schema", session.defaultSchema()));
            rows.add(getRow("timeout", String.valueOf(session.defaultQueryTimeout(TimeUnit.MILLISECONDS))));
            rows.add(getRow("pageSize", String.valueOf(session.defaultPageSize())));

            var props = ((FakeSession) session).properties();

            for (var e : props.entrySet()) {
                rows.add(getRow(e.getKey(), String.valueOf(e.getValue())));
            }

            columns = new ArrayList<>();

            columns.add(new FakeColumnMetadata("name", ColumnType.STRING));
            columns.add(new FakeColumnMetadata("val", ColumnType.STRING));
        } else if ("SELECT META".equals(statement.query())) {
            columns = new ArrayList<>();

            columns.add(new FakeColumnMetadata("bool", ColumnType.BOOLEAN));
            columns.add(new FakeColumnMetadata("int8", ColumnType.INT8));
            columns.add(new FakeColumnMetadata("int16", ColumnType.INT16));
            columns.add(new FakeColumnMetadata("int32", ColumnType.INT32));
            columns.add(new FakeColumnMetadata("int64", ColumnType.INT64));
            columns.add(new FakeColumnMetadata("float", ColumnType.FLOAT));
            columns.add(new FakeColumnMetadata("double", ColumnType.DOUBLE));
            columns.add(new FakeColumnMetadata("decimal", ColumnType.DECIMAL, 1, 2,
                    true, new ColumnOrigin("SCHEMA1", "TBL2", "BIG_DECIMAL")));
            columns.add(new FakeColumnMetadata("date", ColumnType.DATE));
            columns.add(new FakeColumnMetadata("time", ColumnType.TIME));
            columns.add(new FakeColumnMetadata("datetime", ColumnType.DATETIME));
            columns.add(new FakeColumnMetadata("timestamp", ColumnType.TIMESTAMP));
            columns.add(new FakeColumnMetadata("uuid", ColumnType.UUID));
            columns.add(new FakeColumnMetadata("bitmask", ColumnType.BITMASK));
            columns.add(new FakeColumnMetadata("byte_array", ColumnType.BYTE_ARRAY));
            columns.add(new FakeColumnMetadata("period", ColumnType.PERIOD));
            columns.add(new FakeColumnMetadata("duration", ColumnType.DURATION));
            columns.add(new FakeColumnMetadata("number", ColumnType.NUMBER));

            var row = getRow(
                    true,
                    Byte.MIN_VALUE,
                    Short.MIN_VALUE,
                    Integer.MIN_VALUE,
                    Long.MIN_VALUE,
                    1.3f,
                    1.4d,
                    BigDecimal.valueOf(145),
                    LocalDate.of(2001, 2, 3),
                    LocalTime.of(4, 5),
                    LocalDateTime.of(2001, 3, 4, 5, 6),
                    Instant.ofEpochSecond(987),
                    new UUID(0, 0),
                    BitSet.valueOf(new byte[0]),
                    new byte[1],
                    Period.of(10, 9, 8),
                    Duration.ofDays(11),
                    BigInteger.valueOf(42));

            rows = List.of(row);
        } else {
            rows = List.of(getRow(1));
            columns = List.of(new FakeColumnMetadata("col1", ColumnType.INT32));
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ResultSetMetadata metadata() {
        return new ResultSetMetadata() {
            @Override
            public List<ColumnMetadata> columns() {
                return columns;
            }

            @Override
            public int indexOf(String columnName) {
                return 0;
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasRowSet() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public long affectedRows() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasApplied() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<SqlRow> currentPage() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override
    public int currentPageSize() {
        return rows.size();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends AsyncResultSet> fetchNextPage() {
        return CompletableFuture.completedFuture(new FakeAsyncResultSet(session, transaction, statement, arguments));
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return hasMorePages;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @NotNull
    private SqlRow getRow(Object... vals) {
        return new FakeSqlRow(List.of(vals), metadata());
    }

    private static class ColumnOrigin implements ColumnMetadata.ColumnOrigin {
        private final String schemaName;
        private final String tableName;
        private final String columnName;

        public ColumnOrigin(String schemaName, String tableName, String columnName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public String schemaName() {
            return schemaName;
        }

        @Override
        public String tableName() {
            return tableName;
        }

        @Override
        public String columnName() {
            return columnName;
        }
    }
}
