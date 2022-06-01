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

package org.apache.ignite.client.fakes;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.sql.ClientStatement;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

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

        if ("SELECT PROPS".equals(statement.query())) {
            rows = new ArrayList<>();

            rows.add(getRow("schema", statement.defaultSchema()));
            rows.add(getRow("timeout", statement.queryTimeout(TimeUnit.MILLISECONDS)));
            rows.add(getRow("pageSize", statement.pageSize()));

            var props = ((ClientStatement) statement).properties();

            for (var e : props.entrySet()) {
                rows.add(getRow(e.getKey(), e.getValue()));
            }

            columns = new ArrayList<>();

            columns.add(new FakeColumnMetadata("name"));
            columns.add(new FakeColumnMetadata("val"));
        } else {
            rows = new ArrayList<>();
            rows.add(getRow(1));

            columns = new ArrayList<>();
            columns.add(new FakeColumnMetadata("col1"));
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
    public CompletionStage<? extends AsyncResultSet> fetchNextPage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> closeAsync() {
        return null;
    }

    @NotNull
    private SqlRow getRow(Object... vals) {
        var row = mock(SqlRow.class);

        for (int i = 0; i < vals.length; i++) {
            Mockito.when(row.value(i)).thenReturn(vals[i]);
        }

        return row;
    }
}
