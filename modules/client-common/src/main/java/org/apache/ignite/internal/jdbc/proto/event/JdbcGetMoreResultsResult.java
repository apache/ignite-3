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

package org.apache.ignite.internal.jdbc.proto.event;

import static org.apache.ignite.internal.binarytuple.BinaryTupleParser.ORDER;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.sql.ColumnType;

/**
 * JDBC getMoreResults result.
 */
public class JdbcGetMoreResultsResult extends Response {
    /** Cursor ID. */
    private long cursorId;

    /** Query type. */
    private long updCount;

    /** Serialized result rows. */
    private List<ByteBuffer> rowTuples;

    /** Flag indicating the results set has no unfetched results. */
    private boolean last;

    /** Ordered list of types of columns in serialized rows. */
    private List<ColumnType> columnTypes;

    /** Decimal scales in appearance order. Can be empty in case no any decimal columns. */
    private int[] decimalScales;

    boolean isQuery;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcGetMoreResultsResult() {
    }

    /**
     * Constructor.
     *
     * @param hasNext {@code true} if more results are present.
     * @param cursorId Cursor ID.
     */
    public JdbcGetMoreResultsResult(boolean hasNext, long cursorId) {
        hasResults = hasNext;
        this.cursorId = cursorId;
    }

    /**
     * Constructor.
     *
     * @param hasNext {@code true} if more results are present.
     * @param cursorId Cursor ID.
     * @param type Query type.
     * @param items Query result rows.
     * @param last  Flag indicating the query has no unfetched results.
     */
    public JdbcGetMoreResultsResult(
            boolean hasNext,
            long cursorId,
            long updCount,
            List<ColumnType> columnTypes,
            int[] decimalScales,
            List<ByteBuffer> items,
            boolean last,
            boolean isQuery) {
        hasResults = hasNext;
        this.cursorId = cursorId;
        this.updCount = updCount;
        this.columnTypes = columnTypes;
        this.decimalScales = decimalScales;
        this.rowTuples = items;
        this.last = last;
        this.isQuery = isQuery;
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcGetMoreResultsResult(int status, String err) {
        super(status, err);
    }

    /**
     * Get the cursor id.
     *
     * @return Cursor ID.
     */
    public long cursorId() {
        return this.cursorId;
    }

    /**
     * Return query type.
     */
    public long updateCount() {
        return updCount;
    }

    public List<ByteBuffer> items() {
        return rowTuples;
    }

    public boolean last() {
        return last;
    }

    public List<ColumnType> columnTypes() {
        return columnTypes;
    }

    public int[] decimalScales() {
        return decimalScales;
    }

    public boolean isQuery() {
        return isQuery;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults) {
            return;
        }

        packer.packLong(cursorId);
        packer.packLong(updCount);

        packer.packBoolean(last);
        packer.packBoolean(isQuery);

        packer.packIntArray(decimalScales);

        packer.packInt(this.columnTypes.size());
        for (ColumnType columnType : this.columnTypes) {
            packer.packInt(columnType.id());
        }

        packer.packInt(rowTuples.size());
        for (ByteBuffer item : rowTuples) {
            packer.packByteBuffer(item);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        cursorId = unpacker.unpackLong();
        updCount = unpacker.unpackLong();

        last = unpacker.unpackBoolean();
        isQuery = unpacker.unpackBoolean();

        decimalScales = unpacker.unpackIntArray();

        int count = unpacker.unpackInt();
        columnTypes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            columnTypes.add(ColumnType.getById(unpacker.unpackInt()));
        }

        int size = unpacker.unpackInt();
        rowTuples = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            rowTuples.add(ByteBuffer.wrap(unpacker.readBinary()).order(ORDER));
        }
    }
}
