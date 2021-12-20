/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb.index;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.index.IndexBinaryRow;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;

/**
 * {@link IndexBinaryRow} implementation that uses {@link BinaryRow} serialization.
 */
class IndexRowImpl implements IndexRow, IndexBinaryRow {
    private final IndexBinaryRow binRow;

    private final Row row;

    private final SortedIndexDescriptor desc;

    /**
     * Creates index row by index data schema over the bytes.
     *
     * @param binRow Binary presentation of the index row.
     * @param desc   Index descriptor.
     */
    IndexRowImpl(IndexBinaryRow binRow, SortedIndexDescriptor desc) {
        this.binRow = binRow;
        this.desc = desc;

        row = new Row(desc.schema(), new ByteBufferRow(binRow.rowBytes()));
    }

    /** {@inheritDoc} */
    @Override
    public Object value(int idxColOrder) {
        Column c = desc.columns().get(idxColOrder).column();

        switch (c.type().spec()) {
            case INT8:
                return row.byteValueBoxed(c.schemaIndex());

            case INT16:
                return row.shortValueBoxed(c.schemaIndex());

            case INT32:
                return row.intValueBoxed(c.schemaIndex());

            case INT64:
                return row.longValueBoxed(c.schemaIndex());

            case FLOAT:
                return row.floatValueBoxed(c.schemaIndex());

            case DOUBLE:
                return row.doubleValueBoxed(c.schemaIndex());

            case DECIMAL:
                return row.decimalValue(c.schemaIndex());

            case UUID:
                return row.uuidValue(c.schemaIndex());

            case STRING:
                return row.stringValue(c.schemaIndex());

            case BYTES:
                return row.bytesValue(c.schemaIndex());

            case BITMASK:
                return row.bitmaskValue(c.schemaIndex());

            case NUMBER:
                return row.numberValue(c.schemaIndex());

            case DATE:
                return row.dateValue(c.schemaIndex());

            case TIME:
                return row.timeValue(c.schemaIndex());

            case DATETIME:
                return row.dateTimeValue(c.schemaIndex());

            case TIMESTAMP:
                return row.timestampValue(c.schemaIndex());

            default:
                throw new IllegalStateException("Unexpected value: " + c.type().spec());
        }
    }

    /** {@inheritDoc} */
    @Override
    public int columnsCount() {
        return desc.columns().size();
    }

    /** {@inheritDoc} */
    @Override
    public byte[] rowBytes() {
        return binRow.rowBytes();
    }

    /** {@inheritDoc} */
    @Override
    public SearchRow primaryKey() {
        return binRow.primaryKey();
    }
}
