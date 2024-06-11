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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC query execute result.
 */
public class JdbcQuerySingleResult extends Response {
    // === Common attributes ===

    /** Id of the cursor in case it was registered on server. */
    private @Nullable Long cursorId;

    private boolean hasResultSet;

    /** Result is part of multi-statement query, there is at least one more result. */
    private boolean hasNextResult;

    // === Attributes of response with result set ===

    /** Serialized query result rows. Null only when result has no resultSet. */
    private @Nullable List<BinaryTupleReader> rowTuples;

    /** Flag indicating the query has un-fetched results. */
    private boolean hasMoreData;

    /** Ordered list of types of columns in serialized rows. Null only when result has no resultSet. */
    private @Nullable List<ColumnType> columnTypes;

    /** Decimal scales in appearance order. Can be empty in case no any decimal columns, or null when result has no resultSet. */
    private int @Nullable [] decimalScales;

    // === Attributes of response without result set ===

    private long updateCnt = -1;


    /**
     * Constructor.
     */
    public JdbcQuerySingleResult() { }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcQuerySingleResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param cursorId Id of the cursor in case it was registered on server.
     * @param rowTuples Serialized SQL result rows.
     * @param columnTypes Ordered list of types of columns in serialized rows.
     * @param decimalScales Decimal scales in appearance order.
     * @param hasMoreData Flag indicates the query has un-fetched results.
     * @param hasNextResult Flag indicates that current result is part of multi-statement query, there is at least one more result.
     */
    @SuppressWarnings("NullableProblems")
    public JdbcQuerySingleResult(
            @Nullable Long cursorId,
            List<BinaryTupleReader> rowTuples,
            List<ColumnType> columnTypes,
            int[] decimalScales,
            boolean hasMoreData,
            boolean hasNextResult
    ) {
        Objects.requireNonNull(rowTuples);

        this.cursorId = cursorId;
        this.rowTuples = rowTuples;
        this.columnTypes = columnTypes;
        this.decimalScales = decimalScales;

        this.hasMoreData = hasMoreData;
        this.hasNextResult = hasNextResult;

        hasResultSet = true;

        assert decimalScales != null;
    }

    /**
     * Constructor.
     *
     * @param cursorId Id of the cursor in case it was registered on server.
     * @param updateCnt Update count for DML queries.
     * @param hasNextResult Flag indicates that current result is part of multi-statement query, there is at least one more result.
     */
    public JdbcQuerySingleResult(@Nullable Long cursorId, long updateCnt, boolean hasNextResult) {
        this.updateCnt = updateCnt;
        this.cursorId = cursorId;
        this.hasNextResult = hasNextResult;
    }

    /** Return id of the cursor in case it was registered on server, returns null otherwise. */
    public @Nullable Long cursorId() {
        return cursorId;
    }

    /** Return result rows in serialized form, return null if result has no result set. */
    public @Nullable List<BinaryTupleReader> items() {
        return rowTuples;
    }

    /** Return types of columns in serialized rows if result has result set, return null otherwise. */
    public @Nullable List<ColumnType> columnTypes() {
        return columnTypes;
    }

    /** Return decimal scales in appearance order in columns if result has result set, return null otherwise. */
    public int @Nullable [] decimalScales() {
        return decimalScales;
    }

    /** Returns {@code true} if there is more data available in current result set. */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean hasMoreData() {
        return hasMoreData;
    }

    /** Returns {@code true} if result contains rows. */
    public boolean hasResultSet() {
        return hasResultSet;
    }

    /** Returns {@code true} if result is part of multi-statement query and there is at least one more result. */
    public boolean hasNextResult() {
        return hasNextResult;
    }

    /**
     * Get the update count.
     *
     * @return Update count for DML queries.
     */
    public long updateCount() {
        return updateCnt;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!success()) {
            return;
        }

        packer.packLongNullable(cursorId);
        packer.packBoolean(hasResultSet);
        packer.packBoolean(hasNextResult);

        if (!hasResultSet) {
            packer.packLong(updateCnt);

            return;
        }

        assert decimalScales != null;
        assert columnTypes != null;
        assert rowTuples != null;

        packer.packBoolean(hasMoreData);
        packer.packIntArray(decimalScales);

        packer.packInt(this.columnTypes.size());
        for (ColumnType columnType : this.columnTypes) {
            packer.packInt(columnType.id());
        }

        packer.packInt(rowTuples.size());

        for (BinaryTupleReader item : rowTuples) {
            packer.packByteBuffer(item.byteBuffer());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!success()) {
            return;
        }

        if (unpacker.tryUnpackNil()) {
            cursorId = null;
        } else {
            cursorId = unpacker.unpackLong();
        }

        hasResultSet = unpacker.unpackBoolean();
        hasNextResult = unpacker.unpackBoolean();

        if (!hasResultSet) {
            updateCnt = unpacker.unpackLong();

            return;
        }

        hasMoreData = unpacker.unpackBoolean();
        decimalScales = unpacker.unpackIntArray();

        int count = unpacker.unpackInt();
        columnTypes = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            columnTypes.add(ColumnType.getById(unpacker.unpackInt()));
        }

        int size = unpacker.unpackInt();

        rowTuples = new ArrayList<>(size);
        for (int rowIdx = 0; rowIdx < size; rowIdx++) {
            rowTuples.add(new BinaryTupleReader(count, unpacker.readBinary()));
        }

    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcQuerySingleResult.class, this);
    }
}
