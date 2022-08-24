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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.isPrefix;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;

/**
 * Comparator implementation for comparing {@link BinaryTuple}s on a per-column basis.
 */
public class BinaryTupleComparator implements Comparator<ByteBuffer> {
    private final SortedIndexDescriptor descriptor;

    /**
     * Creates a comparator for a Sorted Index identified by the given descriptor.
     */
    public BinaryTupleComparator(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public int compare(ByteBuffer buffer1, ByteBuffer buffer2) {
        assert buffer1.order() == ByteOrder.LITTLE_ENDIAN;
        assert buffer2.order() == ByteOrder.LITTLE_ENDIAN;

        BinaryTupleSchema schema = descriptor.binaryTupleSchema();

        InternalTuple tuple1;
        InternalTuple tuple2;
        int columnsToCompare;

        if (isPrefix(buffer1)) {
            assert !isPrefix(buffer2);

            tuple1 = new BinaryTuplePrefix(schema, buffer1);
            tuple2 = new BinaryTuple(schema, buffer2);
            columnsToCompare = tuple1.count();
        } else if (isPrefix(buffer2)) {
            assert !isPrefix(buffer1);

            tuple1 = new BinaryTuple(schema, buffer1);
            tuple2 = new BinaryTuplePrefix(schema, buffer2);
            columnsToCompare = tuple2.count();
        } else {
            tuple1 = new BinaryTuple(schema, buffer1);
            tuple2 = new BinaryTuple(schema, buffer2);
            columnsToCompare = descriptor.indexColumns().size();
        }

        assert columnsToCompare <= descriptor.indexColumns().size();

        for (int i = 0; i < columnsToCompare; i++) {
            ColumnDescriptor columnDescriptor = descriptor.indexColumns().get(i);

            int compare = compareField(tuple1, tuple2, i);

            if (compare != 0) {
                return columnDescriptor.asc() ? compare : -compare;
            }
        }

        return 0;
    }

    /**
     * Compares individual fields of two tuples.
     */
    private int compareField(InternalTuple tuple1, InternalTuple tuple2, int index) {
        boolean tuple1HasNull = tuple1.hasNullValue(index);
        boolean tuple2HasNull = tuple2.hasNullValue(index);

        if (tuple1HasNull && tuple2HasNull) {
            return 0;
        } else if (tuple1HasNull) {
            return -1;
        } else if (tuple2HasNull) {
            return 1;
        }

        ColumnDescriptor columnDescriptor = descriptor.indexColumns().get(index);

        NativeTypeSpec typeSpec = columnDescriptor.type().spec();

        switch (typeSpec) {
            case INT8:
                return Byte.compare(tuple1.byteValue(index), tuple2.byteValue(index));

            case INT16:
                return Short.compare(tuple1.shortValue(index), tuple2.shortValue(index));

            case INT32:
                return Integer.compare(tuple1.intValue(index), tuple2.intValue(index));

            case INT64:
                return Long.compare(tuple1.longValue(index), tuple2.longValue(index));

            case FLOAT:
                return Float.compare(tuple1.floatValue(index), tuple2.floatValue(index));

            case DOUBLE:
                return Double.compare(tuple1.doubleValue(index), tuple2.doubleValue(index));

            case BYTES:
                return Arrays.compare(tuple1.bytesValue(index), tuple2.bytesValue(index));

            case BITMASK:
                return Arrays.compare(tuple1.bitmaskValue(index).toLongArray(), tuple2.bitmaskValue(index).toLongArray());

            // all other types implement Comparable
            case DECIMAL:
            case UUID:
            case STRING:
            case NUMBER:
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
                return ((Comparable) typeSpec.objectValue(tuple1, index)).compareTo(typeSpec.objectValue(tuple2, index));

            default:
                throw new IllegalArgumentException(String.format(
                        "Unsupported column type in binary tuple comparator. Column name: %s, column type: %s",
                        columnDescriptor.name(), columnDescriptor.type()
                ));
        }
    }
}
