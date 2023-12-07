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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

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

    private final FakeIgniteSql sql;

    /**
     * Constructor.
     *
     * @param session Session.
     * @param transaction Transaction.
     * @param statement Statement.
     * @param arguments Arguments.
     */
    public FakeAsyncResultSet(Session session, Transaction transaction, Statement statement, Object[] arguments, FakeIgniteSql sql) {
        assert session != null;
        assert statement != null;

        this.session = session;
        this.transaction = transaction;
        this.statement = statement;
        this.arguments = arguments;
        this.sql = sql;

        hasMorePages = session.property("hasMorePages") != null;

        if ("SELECT PROPS".equals(statement.query())) {
            rows = new ArrayList<>();

            rows.add(getRow("schema", session.defaultSchema()));
            rows.add(getRow("timeout", String.valueOf(session.defaultQueryTimeout(TimeUnit.MILLISECONDS))));
            rows.add(getRow("pageSize", String.valueOf(session.defaultPageSize())));

            var props = ((FakeSession) session).properties();

            for (var e : props.entrySet()) {
                rows.add(getRow(e.getKey(), e.getValue()));
            }

            columns = new ArrayList<>();

            columns.add(new FakeColumnMetadata("name", ColumnType.STRING));
            columns.add(new FakeColumnMetadata("val", ColumnType.STRING));
        } else if ("SELECT META".equals(statement.query())) {
            columns = new ArrayList<>();

            columns.add(new FakeColumnMetadata("BOOL", ColumnType.BOOLEAN));
            columns.add(new FakeColumnMetadata("INT8", ColumnType.INT8));
            columns.add(new FakeColumnMetadata("INT16", ColumnType.INT16));
            columns.add(new FakeColumnMetadata("INT32", ColumnType.INT32));
            columns.add(new FakeColumnMetadata("INT64", ColumnType.INT64));
            columns.add(new FakeColumnMetadata("FLOAT", ColumnType.FLOAT));
            columns.add(new FakeColumnMetadata("DOUBLE", ColumnType.DOUBLE));
            columns.add(new FakeColumnMetadata("DECIMAL", ColumnType.DECIMAL, 1, 2,
                    true, new ColumnOrigin("SCHEMA1", "TBL2", "BIG_DECIMAL")));
            columns.add(new FakeColumnMetadata("DATE", ColumnType.DATE));
            columns.add(new FakeColumnMetadata("TIME", ColumnType.TIME));
            columns.add(new FakeColumnMetadata("DATETIME", ColumnType.DATETIME));
            columns.add(new FakeColumnMetadata("TIMESTAMP", ColumnType.TIMESTAMP));
            columns.add(new FakeColumnMetadata("UUID", ColumnType.UUID));
            columns.add(new FakeColumnMetadata("BITMASK", ColumnType.BITMASK));
            columns.add(new FakeColumnMetadata("BYTE_ARRAY", ColumnType.BYTE_ARRAY));
            columns.add(new FakeColumnMetadata("PERIOD", ColumnType.PERIOD));
            columns.add(new FakeColumnMetadata("DURATION", ColumnType.DURATION));
            columns.add(new FakeColumnMetadata("NUMBER", ColumnType.NUMBER));

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
        } else if ("SELECT LAST SCRIPT".equals(statement.query())) {
            rows = List.of(getRow(sql.lastScript));
            columns = List.of(new FakeColumnMetadata("script", ColumnType.STRING));
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
        return CompletableFuture.completedFuture(new FakeAsyncResultSet(session, transaction, statement, arguments, sql));
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return hasMorePages;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        return nullCompletedFuture();
    }

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
