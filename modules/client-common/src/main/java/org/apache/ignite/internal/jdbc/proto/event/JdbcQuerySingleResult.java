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

/**
 * JDBC query execute result.
 */
public class JdbcQuerySingleResult extends Response {
    /** Cursor ID. */
    private Long cursorId;

    /** Serialized query result rows. */
    private List<BinaryTupleReader> rowTuples;

    /** Flag indicating the query has no unfetched results. */
    private boolean last;

    /** Flag indicating the query is SELECT/EXPLAIN query. {@code false} for DML/DDL/TX queries. */
    private boolean isQuery;

    /** Update count. */
    private long updateCnt;

    /** Ordered list of types of columns in serialized rows. */
    private List<ColumnType> columnTypes;

    /** Decimal scales in appearance order. Can be empty in case no any decimal columns. */
    private int[] decimalScales;

    /** {@code true} if results are available, {@code false} otherwise. */
    private boolean resultsAvailable;

    /**
     * Constructor.
     */
    public JdbcQuerySingleResult() {
        resultsAvailable = false;
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcQuerySingleResult(int status, String err) {
        super(status, err);

        resultsAvailable = false;
    }

    /**
     * Constructor.
     *
     * @param cursorId Cursor ID.
     * @param rowTuples Serialized SQL result rows.
     * @param columnTypes Ordered list of types of columns in serialized rows.
     * @param decimalScales Decimal scales in appearance order.
     * @param last     Flag indicates the query has no unfetched results.
     */
    public JdbcQuerySingleResult(long cursorId, List<BinaryTupleReader> rowTuples, List<ColumnType> columnTypes, int[] decimalScales,
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
        resultsAvailable = true;

        assert decimalScales != null;
    }

    /**
     * Constructor.
     *
     * @param updateCnt Update count for DML queries.
     */
    public JdbcQuerySingleResult(long cursorId, long updateCnt) {
        super();

        this.updateCnt = updateCnt;
        this.cursorId = cursorId;

        hasResults = false;
        resultsAvailable = true;
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
    public List<BinaryTupleReader> items() {
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
     */
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

    /** Results availability flag.
     * If no more results available, returns {@code false}
     */
    public boolean resultAvailable() {
        return resultsAvailable;
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

        packer.packBoolean(resultsAvailable);
        if (resultsAvailable) {
            packer.packLong(updateCnt);

            if (cursorId != null) {
                packer.packLong(cursorId);
            } else {
                packer.packNil();
            }
        }

        if (!hasResults) {
            return;
        }

        packer.packBoolean(isQuery);
        packer.packBoolean(last);

        packer.packIntArray(decimalScales);

        packer.packInt(this.columnTypes.size());
        for (int i = 0; i < this.columnTypes.size(); i++) {
            packer.packInt(this.columnTypes.get(i).id());
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
        resultsAvailable = unpacker.unpackBoolean();
        if (resultsAvailable) {
            updateCnt = unpacker.unpackLong();

            if (unpacker.tryUnpackNil()) {
                cursorId = null;
            } else {
                cursorId = unpacker.unpackLong();
            }
        }

        if (!hasResults) {
            return;
        }

        isQuery = unpacker.unpackBoolean();
        last = unpacker.unpackBoolean();

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
