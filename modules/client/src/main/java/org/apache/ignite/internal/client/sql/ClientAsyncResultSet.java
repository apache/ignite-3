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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;
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
    private final ResultSetMetadata metadata;

    /** Marshaller. Not null when object mapping is used. */
    @Nullable
    private final Marshaller marshaller;

    /** Mapper. */
    @Nullable
    private final Mapper<T> mapper;

    /** Rows. */
    private volatile List<T> rows;

    /** More pages flag. */
    private volatile boolean hasMorePages;

    /** Closed flag. */
    private volatile boolean closed;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param in Unpacker.
     * @param mapper Mapper.
     */
    ClientAsyncResultSet(ClientChannel ch, MarshallersProvider marshallers, ClientMessageUnpacker in, @Nullable Mapper<T> mapper) {
        this.ch = ch;

        resourceId = in.tryUnpackNil() ? null : in.unpackLong();
        hasRowSet = in.unpackBoolean();
        hasMorePages = in.unpackBoolean();
        wasApplied = in.unpackBoolean();
        affectedRows = in.unpackLong();
        metadata = hasRowSet ? new ClientResultSetMetadata(in) : null;

        this.mapper = mapper;
        marshaller = metadata != null && mapper != null && mapper.targetType() != SqlRow.class
                ? marshaller(metadata, marshallers, mapper)
                : null;

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
    public Iterable<T> currentPage() {
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
    public CompletableFuture<? extends AsyncResultSet<T>> fetchNextPage() {
        requireResultSet();

        if (closed || !hasMorePages()) {
            return CompletableFuture.failedFuture(new CursorClosedException());
        }

        return ch.serviceAsync(
                ClientOp.SQL_CURSOR_NEXT_PAGE,
                w -> w.out().packLong(resourceId),
                r -> {
                    readRows(r.in());
                    hasMorePages = r.in().unpackBoolean();

                    if (!hasMorePages) {
                        // When last page is fetched, server closes the cursor.
                        closed = true;
                    }

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
    public CompletableFuture<Void> closeAsync() {
        if (resourceId == null || closed) {
            return nullCompletedFuture();
        }

        closed = true;

        return ch.serviceAsync(ClientOp.SQL_CURSOR_CLOSE, w -> w.out().packLong(resourceId), null);
    }

    private void requireResultSet() {
        if (!hasRowSet()) {
            throw new NoRowSetExpectedException();
        }
    }

    private void readRows(ClientMessageUnpacker in) {
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
                throw new IgniteException(
                        Client.CONFIGURATION_ERR,
                        "Failed to map SQL result set to type '" + mapper.targetType() + "': " + e.getMessage(),
                        e);
            }
        }

        rows = Collections.unmodifiableList(res);
    }

    private static Object readValue(BinaryTupleReader in, int idx, ColumnMetadata col) {
        if (in.hasNullValue(idx)) {
            return null;
        }

        switch (col.type()) {
            case BOOLEAN:
                return in.byteValue(idx) != 0;

            case INT8:
                return in.byteValue(idx);

            case INT16:
                return in.shortValue(idx);

            case INT32:
                return in.intValue(idx);

            case INT64:
                return in.longValue(idx);

            case FLOAT:
                return in.floatValue(idx);

            case DOUBLE:
                return in.doubleValue(idx);

            case DECIMAL:
                return in.decimalValue(idx, col.scale());

            case DATE:
                return in.dateValue(idx);

            case TIME:
                return in.timeValue(idx);

            case DATETIME:
                return in.dateTimeValue(idx);

            case TIMESTAMP:
                return in.timestampValue(idx);

            case UUID:
                return in.uuidValue(idx);

            case BITMASK:
                return in.bitmaskValue(idx);

            case STRING:
                return in.stringValue(idx);

            case BYTE_ARRAY:
                return in.bytesValue(idx);

            case PERIOD:
                return in.periodValue(idx);

            case DURATION:
                return in.durationValue(idx);

            case NUMBER:
                return in.numberValue(idx);

            default:
                throw new UnsupportedOperationException("Unsupported column type: " + col.type());
        }
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
}
