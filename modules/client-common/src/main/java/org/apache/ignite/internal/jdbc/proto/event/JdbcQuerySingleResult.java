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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.sql.ColumnType;

/**
 * JDBC query execute result.
 */
public class JdbcQuerySingleResult extends Response {
    /** Cursor ID. */
    private Long cursorId;

    /** Serialized query result rows. */
    private List<BinaryTuple> rowTuples;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT query. {@code false} for DML/DDL queries. */
    private boolean isQuery;

    /** Update count. */
    private long updateCnt;

    /** Ordered list of types of columns in serialized rows. */
    private List<ColumnType> columnTypes;

    /** Decimal scales in appearance order. Can be empty in case no any decimal columns. */
    private int[] decimalScales;

    /**
     * Constructor. For deserialization purposes only.
     */
    public JdbcQuerySingleResult() {
    }

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
     * @param cursorId Cursor ID.
     * @param rowTuples Serialized SQL result rows.
     * @param last     Flag indicates the query has no unfetched results.
     */
    public JdbcQuerySingleResult(long cursorId, List<BinaryTuple> rowTuples, List<ColumnType> columnTypes, int[] decimalScales,
            boolean last) {
        super();

        Objects.requireNonNull(rowTuples);

        this.cursorId = cursorId;
        this.rowTuples = rowTuples;
        this.columnTypes = columnTypes;
        this.decimalScales = decimalScales;

        this.last = last;
        this.isQuery = true;

        hasResults = true;

        assert decimalScales != null;
    }

    /**
     * Constructor.
     *
     * @param updateCnt Update count for DML queries.
     */
    public JdbcQuerySingleResult(long updateCnt) {
        super();

        this.last = true;
        this.isQuery = false;
        this.updateCnt = updateCnt;
        this.rowTuples = Collections.emptyList();
        columnTypes = Collections.emptyList();
        this.decimalScales = ArrayUtils.INT_EMPTY_ARRAY;
        hasResults = true;
    }

    /**
     * Get the cursor id.
     *
     * @return Cursor ID.
     */
    public Long cursorId() {
        return cursorId;
    }

    /**
     * Get the items.
     *
     * @return Serialized query result rows.
     */
    public List<BinaryTuple> items() {
        return rowTuples;
    }

    /**
     * Types of columns in serialized rows.
     *
     * @return Ordered list of types of columns in serialized rows.
     */
    public List<ColumnType> columnTypes() {
        return columnTypes;
    }

    /**
     * Decimal scales.
     *
     * @return Decimal scales in appearance order in columns. Can be empty in case no any decimal columns.
     * */
    public int[] decimalScales() {
        return decimalScales;
    }

    /**
     * Get the last flag.
     *
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean last() {
        return last;
    }

    /**
     * Get the isQuery flag.
     *
     * @return Flag indicating the query is SELECT query. {@code false} for DML/DDL queries.
     */
    public boolean isQuery() {
        return isQuery;
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

        if (!hasResults) {
            return;
        }

        if (cursorId != null) {
            packer.packLong(cursorId);
        } else {
            packer.packNil();
        }

        packer.packBoolean(isQuery);
        packer.packLong(updateCnt);
        packer.packBoolean(last);

        packer.packIntArray(decimalScales);

        int[] columnTypes = new int[this.columnTypes.size()];
        for (int i = 0; i < this.columnTypes.size(); i++) {
            columnTypes[i] = this.columnTypes.get(i).id();
        }

        packer.packIntArray(columnTypes);

        packer.packInt(rowTuples.size());

        for (BinaryTuple item : rowTuples) {
            packer.packByteBuffer(item.byteBuffer());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        if (unpacker.tryUnpackNil()) {
            cursorId = null;
        } else {
            cursorId = unpacker.unpackLong();
        }
        isQuery = unpacker.unpackBoolean();
        updateCnt = unpacker.unpackLong();
        last = unpacker.unpackBoolean();

        decimalScales = unpacker.unpackIntArray();
        int[] columnTypeIds = unpacker.unpackIntArray();

        columnTypes = new ArrayList<>(columnTypeIds.length);
        for (int columnType : columnTypeIds) {
            columnTypes.add(ColumnType.getById(columnType));
        }

        int size = unpacker.unpackInt();

        rowTuples = new ArrayList<>(size);
        for (int rowIdx = 0; rowIdx < size; rowIdx++) {
            rowTuples.add(new BinaryTuple(columnTypes.size(), unpacker.readBinary()));
        }

    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcQuerySingleResult.class, this);
    }
}
