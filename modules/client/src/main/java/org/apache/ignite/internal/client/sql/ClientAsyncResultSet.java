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

package org.apache.ignite.internal.client.sql;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Client async result set.
 */
class ClientAsyncResultSet<T> implements AsyncResultSet<T> {
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
    private final @Nullable ResultSetMetadata metadata;

    private final @Nullable ClientPartitionAwarenessMetadata partitionAwarenessMetadata;

    /** Marshaller. Not null when object mapping is used. */
    @Nullable
    private final Marshaller marshaller;

    /** Mapper. */
    @Nullable
    private final Mapper<T> mapper;

    /** Current page. */
    @Nullable
    private volatile Page<T> page;

    /** Closed flag. */
    private volatile boolean closed;

    /** Prefetched next page future. */
    private volatile CompletableFuture<Page<T>> nextPageFut;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param marshallers Used to create marshaller in case result is for query and {@code mapper} is provided.
     * @param in Unpacker.
     * @param mapper Mapper.
     * @param partitionAwarenessEnabled Whether partitions awareness is enabled, hence response may contain related metadata.
     * @param sqlDirectMappingSupported Whether direct mapping is supported, hence response may contain additional metadata.
     */
    ClientAsyncResultSet(
            ClientChannel ch,
            MarshallersProvider marshallers,
            ClientMessageUnpacker in,
            @Nullable Mapper<T> mapper,
            boolean partitionAwarenessEnabled,
            boolean sqlDirectMappingSupported
    ) {
        this.ch = ch;

        resourceId = in.tryUnpackNil() ? null : in.unpackLong();
        hasRowSet = in.unpackBoolean();
        var hasMorePages = in.unpackBoolean();
        wasApplied = in.unpackBoolean();
        affectedRows = in.unpackLong();
        metadata = ClientResultSetMetadata.read(in);

        if (partitionAwarenessEnabled && !in.tryUnpackNil()) {
            partitionAwarenessMetadata = ClientPartitionAwarenessMetadata.read(in, sqlDirectMappingSupported);
        } else {
            partitionAwarenessMetadata = null;
        }

        this.mapper = mapper;
        marshaller = metadata != null && mapper != null && mapper.targetType() != SqlRow.class
                ? marshaller(metadata, marshallers, mapper)
                : null;

        if (hasRowSet) {
            assert metadata != null : "Metadata must be present when row set is available";
            List<T> rows = readRows(in, metadata, marshaller, mapper);
            page = new Page<>(rows, hasMorePages);

            if (hasMorePages) {
                assert resourceId != null : "Resource id must be present when more pages are available";
                nextPageFut = fetchNextPageInternal(ch, resourceId, marshaller, mapper, metadata);
            } else {
                // When last page is fetched, server closes the cursor.
                closed = true;
            }
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
    public Iterable<T> currentPage() {
        requireResultSet();

        Page<T> p = page;
        assert p != null : "Page must be present when row set is available";
        return p.rows;
    }

    /** {@inheritDoc} */
    @Override
    public int currentPageSize() {
        requireResultSet();

        Page<T> p = page;
        assert p != null : "Page must be present when row set is available";
        return p.rows.size();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends AsyncResultSet<T>> fetchNextPage() {
        requireResultSet();

        if (closed || !hasMorePages()) {
            return CompletableFuture.failedFuture(new CursorClosedException());
        }

        return nextPageFut.thenApply(p -> {
            page = p;

            if (p.hasMorePages()) {
                assert resourceId != null : "Resource id must be present when more pages are available";
                nextPageFut = fetchNextPageInternal(ch, resourceId, marshaller, mapper, metadata);
            } else {
                // When last page is fetched, server closes the cursor.
                closed = true;
            }

            return this;
        });
    }

    private static <T> CompletableFuture<Page<T>> fetchNextPageInternal(
            ClientChannel ch,
            long resourceId,
            @Nullable Marshaller marshaller,
            @Nullable Mapper<T> mapper,
            @Nullable ResultSetMetadata metadata) {
        return ch.serviceAsync(
                ClientOp.SQL_CURSOR_NEXT_PAGE,
                w -> w.out().packLong(resourceId),
                r -> {
                    assert metadata != null : "Metadata must be present when row set is available";
                    List<T> rows = readRows(r.in(), metadata, marshaller, mapper);
                    boolean hasMorePages = r.in().unpackBoolean();

                    return new Page<>(rows, hasMorePages);
                });
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasMorePages() {
        Page<T> p = page;
        return p != null && p.hasMorePages();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        if (resourceId == null || closed) {
            return nullCompletedFuture();
        }

        closed = true;

        return ch.serviceAsync(ClientOp.SQL_CURSOR_CLOSE, w -> w.out().packLong(resourceId), null)
                .exceptionally(t -> {
                    Throwable cause = unwrapCause(t);

                    if (cause instanceof IgniteException) {
                        IgniteException igniteEx = (IgniteException) cause;

                        if (igniteEx.code() == Client.RESOURCE_NOT_FOUND_ERR) {
                            throw new IgniteException(
                                    Client.RESOURCE_NOT_FOUND_ERR,
                                    "Failed to find cursor with id: " + resourceId + ". Cursor might have been closed concurrently.",
                                    cause);
                        } else if (cause instanceof IgniteClientConnectionException) {
                            // Connection lost - cursor is closed on the server side.
                            return null;
                        }
                    }

                    throw new IgniteException(Common.INTERNAL_ERR, "Failed to close SQL cursor: " + t.getMessage(), t);
                })
                .thenApply(ignore -> null);
    }

    @Nullable ClientPartitionAwarenessMetadata partitionAwarenessMetadata() {
        return partitionAwarenessMetadata;
    }

    private void requireResultSet() {
        if (!hasRowSet()) {
            throw new NoRowSetExpectedException();
        }
    }

    private static <T> List<T> readRows(
            ClientMessageUnpacker in,
            ResultSetMetadata metadata,
            @Nullable Marshaller marshaller,
            @Nullable Mapper<?> mapper) {
        int size = in.unpackInt();
        int rowSize = metadata.columns().size();

        var res = new ArrayList<T>(size);

        if (marshaller == null) {
            for (int i = 0; i < size; i++) {
                var tupleReader = new BinaryTupleReader(rowSize, in.readBinary());

                res.add((T) new ClientSqlRow(tupleReader, metadata));
            }
        } else {
            try {
                for (int i = 0; i < size; i++) {
                    var tupleReader = new BinaryTupleReader(rowSize, in.readBinaryUnsafe());
                    var reader = new ClientMarshallerReader(tupleReader, null, TuplePart.KEY_AND_VAL);

                    res.add((T) marshaller.readObject(reader, null));
                }
            } catch (MarshallerException e) {
                assert mapper != null;
                throw new MarshallerException(
                        "Failed to map SQL result set to type '" + mapper.targetType() + "': " + e.getMessage(),
                        e);
            }
        }

        return Collections.unmodifiableList(res);
    }

    private static <T> Marshaller marshaller(ResultSetMetadata metadata, MarshallersProvider marshallers, Mapper<T> mapper) {
        var schemaColumns = new ClientColumn[metadata.columns().size()];
        List<ColumnMetadata> columns = metadata.columns();

        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata metaColumn = columns.get(i);

            var schemaColumn = new ClientColumn(
                    metaColumn.name(),
                    metaColumn.type(),
                    metaColumn.nullable(),
                    i,
                    -1,
                    -1,
                    i,
                    metaColumn.scale(),
                    metaColumn.precision());

            schemaColumns[i] = schemaColumn;
        }

        var schema = new ClientSchema(0, schemaColumns, marshallers);
        return schema.getMarshaller(mapper);
    }

    private static class Page<T> {
        private final List<T> rows;
        private final boolean hasMorePages;

        Page(List<T> rows, boolean hasMorePages) {
            this.rows = rows;
            this.hasMorePages = hasMorePages;
        }

        public List<T> rows() {
            return rows;
        }

        public boolean hasMorePages() {
            return hasMorePages;
        }
    }
}
