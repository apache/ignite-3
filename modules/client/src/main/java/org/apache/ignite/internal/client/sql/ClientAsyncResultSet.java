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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
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
    private final boolean wasApplied;

    /** */
    private final long affectedRows;

    /** */
    private final ResultSetMetadata metadata;

    /** */
    private List<SqlRow> rows;

    /** */
    private final boolean hasMorePages;

    /**
     * Constructor.
     *
     * @param in Unpacker.
     */
    public ClientAsyncResultSet(ClientMessageUnpacker in) {
        resourceId = in.tryUnpackNil() ? null : in.unpackLong();
        hasRowSet = in.unpackBoolean();
        hasMorePages = in.unpackBoolean();
        wasApplied = in.unpackBoolean();
        affectedRows = in.unpackLong();

        metadata = new ClientResultSetMetadata(in);

        if (hasRowSet) {
            readRows(in);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ResultSetMetadata metadata() {
        return metadata;
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

    private void readRows(ClientMessageUnpacker in) {
        int size = in.unpackArrayHeader();
        int rowSize = metadata.columns().size();

        var res = new ArrayList<SqlRow>(size);

        for (int i = 0; i < size; i++) {
            var row = new ArrayList<>(rowSize);

            for (int j = 0; j < rowSize; j++) {
                // TODO: IGNITE-17052 Unpack according to metadata type.
                row.add(in.unpackObjectWithType());
            }

            res.add(new ClientSqlRow(row));
        }

        rows = Collections.unmodifiableList(res);
    }
}
