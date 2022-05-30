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

package org.apache.ignite.internal.client.sql;

import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Client async result set.
 */
class ClientAsyncResultSet implements AsyncResultSet {
    /** */
    private final Long resourceId;

    /** */
    private final boolean hasRowSet;

    /** */
    private final boolean hasMorePages;

    /** */
    private final boolean wasApplied;

    /** */
    private final long affectedRows;

    /** */
    private final List<SqlRow> rows;

    /**
     * Constructor.
     *
     * @param resourceId Resource id.
     * @param hasRowSet Row set flag.
     * @param hasMorePages More pages flag.
     * @param wasApplied Applied flag.
     * @param rows Rows.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClientAsyncResultSet(
            Long resourceId,
            boolean hasRowSet,
            boolean hasMorePages,
            boolean wasApplied,
            long affectedRows,
            @Nullable List<SqlRow> rows) {
        this.resourceId = resourceId;
        this.hasRowSet = hasRowSet;
        this.hasMorePages = hasMorePages;
        this.wasApplied = wasApplied;
        this.affectedRows = affectedRows;
        this.rows = rows;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ResultSetMetadata metadata() {
        // TODO: IGNITE-17052
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasRowSet() {
        return hasRowSet;
    }

    /** {@inheritDoc} */
    @Override
    public long affectedRows() {
        return affectedRows;
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasApplied() {
        return wasApplied;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Override
    public Iterable<SqlRow> currentPage() {
        requireResultSet();

        return rows;
    }

    /** {@inheritDoc} */
    @Override
    public int currentPageSize() {
        requireResultSet();

        return rows.size();
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<? extends AsyncResultSet> fetchNextPage() {
        requireResultSet();

        // TODO
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return hasMorePages;
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> closeAsync() {
        // TODO
        return null;
    }

    private void requireResultSet() {
        if (!hasRowSet()) {
            throw new NoRowSetExpectedException("Query has no result set");
        }
    }
}
