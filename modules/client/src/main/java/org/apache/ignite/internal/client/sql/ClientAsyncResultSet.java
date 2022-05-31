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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Client async result set.
 */
class ClientAsyncResultSet implements AsyncResultSet {
    /** Channel. */
    private final ClientChannel ch;

    /** Resource id. */
    private final Long resourceId;

    /** Row set flag. */
    private final boolean hasRowSet;

    /** Applied flag. */
    private final boolean wasApplied;

    /** Affected rows. */
    private final long affectedRows;

    /** Metadata. */
    private final ResultSetMetadata metadata;

    /** Rows. */
    private volatile List<SqlRow> rows;

    /** More pages flag. */
    private volatile boolean hasMorePages;

    /** Closed flag. */
    private volatile boolean closed;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param in Unpacker.
     */
    public ClientAsyncResultSet(ClientChannel ch, ClientMessageUnpacker in) {
        this.ch = ch;

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

        if (closed) {
            return CompletableFuture.failedFuture(new IgniteClientException("Cursor is closed."));
        }

        if (!hasMorePages()) {
            return CompletableFuture.failedFuture(new IgniteClientException("No more pages."));
        }

        return ch.serviceAsync(
                ClientOp.SQL_CURSOR_NEXT_PAGE,
                w -> w.out().packLong(resourceId),
                r -> {
                    readRows(r.in());
                    hasMorePages = r.in().unpackBoolean();

                    return this;
                });
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        return resourceId != null && hasMorePages;
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<Void> closeAsync() {
        if (resourceId == null || closed) {
            return CompletableFuture.completedFuture(null);
        }

        closed = true;

        return ch.serviceAsync(ClientOp.SQL_CURSOR_CLOSE, w -> w.out().packLong(resourceId), null);
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
