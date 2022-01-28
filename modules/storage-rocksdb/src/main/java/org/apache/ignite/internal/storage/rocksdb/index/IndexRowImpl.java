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
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;

/**
 * {@link IndexRow} implementation.
 */
class IndexRowImpl implements IndexRow {
    private final Row keyRow;

    private final byte[] valBytes;

    private final SortedIndexDescriptor desc;

    /**
     * Creates index row by index data schema over the bytes.
     *
     * @param desc   Index descriptor.
     */
    IndexRowImpl(byte[] keyBytes, byte[] valBytes, SortedIndexDescriptor desc) {
        this.desc = desc;

        keyRow = new Row(desc.schema(), new ByteBufferRow(keyBytes));
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override
    public Object value(int idxColOrder) {
        Column c = desc.columns().get(idxColOrder).column();

        switch (c.type().spec()) {
            case INT8:
                return keyRow.byteValueBoxed(c.schemaIndex());

            case INT16:
                return keyRow.shortValueBoxed(c.schemaIndex());

            case INT32:
                return keyRow.intValueBoxed(c.schemaIndex());

            case INT64:
                return keyRow.longValueBoxed(c.schemaIndex());

            case FLOAT:
                return keyRow.floatValueBoxed(c.schemaIndex());

            case DOUBLE:
                return keyRow.doubleValueBoxed(c.schemaIndex());

            case DECIMAL:
                return keyRow.decimalValue(c.schemaIndex());

            case UUID:
                return keyRow.uuidValue(c.schemaIndex());

            case STRING:
                return keyRow.stringValue(c.schemaIndex());

            case BYTES:
                return keyRow.bytesValue(c.schemaIndex());

            case BITMASK:
                return keyRow.bitmaskValue(c.schemaIndex());

            case NUMBER:
                return keyRow.numberValue(c.schemaIndex());

            case DATE:
                return keyRow.dateValue(c.schemaIndex());

            case TIME:
                return keyRow.timeValue(c.schemaIndex());

            case DATETIME:
                return keyRow.dateTimeValue(c.schemaIndex());

            case TIMESTAMP:
                return keyRow.timestampValue(c.schemaIndex());

            default:
                throw new IllegalStateException("Unexpected value: " + c.type().spec());
        }
    }

    /** {@inheritDoc} */
    @Override
    public BinaryRow primaryKey() {
        return new ByteBufferRow(valBytes);
    }

    @Override
    public int partition() {
        return 0;
    }
}
