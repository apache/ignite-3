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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.PREFIX_FLAG;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.compareAsBytes;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.compareAsString;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.compareAsTimestamp;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.compareAsUuid;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.compareFieldValue;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.equalityFlag;
import static org.apache.ignite.internal.schema.BinaryTupleComparatorUtils.isFlagSet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser.Readability;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;

/**
 * Matcher for comparing {@link BinaryTuple}s on a per-column basis.
 *
 * <p>This comparator is used to compare BinaryTuples. The first tuple has to be an ordinal tuple that is gotten from persistent.
 * The second tuple can be {@link BinaryTuplePrefix}. The mather assumes that the first tuple may have been written in the
 * buffer partially. If the length of the tuple buffer is not enough to do a comparison, the comparator returns {@code 0}.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class PartialBinaryTupleMatcher {
    private final List<CatalogColumnCollation> columnCollations;
    private final List<NativeType> columnTypes;

    /**
     * Creates BinaryTuple comparator.
     *
     * @param columnCollations Columns collations.
     * @param columnTypes Column types in order, which is defined in BinaryTuple schema.
     */
    public PartialBinaryTupleMatcher(
            List<CatalogColumnCollation> columnCollations,
            List<NativeType> columnTypes
    ) {
        this.columnCollations = columnCollations;
        this.columnTypes = columnTypes;
    }

    /**
     * Compares two binary tuples represented as ByteBuffers to determine their relative ordering.
     * The method takes into account tuple prefixes (the prefix writing structure is available for second buffer only), column types, and
     * other configurations to compare the tuples up to the number of elements specified in the schema. If one of the tuples is a prefix,
     * specific comparison rules leveraging the equality flag are applied.
     *
     * @param buffer1 The first ByteBuffer to compare must be in LITTLE_ENDIAN byte order.
     * @param buffer2 The second ByteBuffer to compare must be in LITTLE_ENDIAN byte order and can contain a prefix {@see PREFIX_FLAG}.
     * @return A positive integer if the first buffer is greater than the second one, zero if the first buffer equals the second one or its
     *         bytes are not enough to compare, and a negative integer in other cases.
     */
    public int match(ByteBuffer buffer1, ByteBuffer buffer2) {
        assert buffer1.order() == ByteOrder.LITTLE_ENDIAN;
        assert buffer2.order() == ByteOrder.LITTLE_ENDIAN;

        boolean isBuffer1Prefix = isFlagSet(buffer1, PREFIX_FLAG);
        boolean isBuffer2Prefix = isFlagSet(buffer2, PREFIX_FLAG);

        int numElements = columnTypes.size();

        assert !isBuffer1Prefix : "An inline tuple must not contain a prefix.";

        BinaryTupleReader tuple1 = new BinaryTuple(numElements, buffer1, UnsafeByteBufferAccessor::new);

        BinaryTupleReader tuple2 = isBuffer2Prefix ? new BinaryTuplePrefix(numElements, buffer2)
                : new BinaryTuple(numElements, buffer2, UnsafeByteBufferAccessor::new);

        int columnsToCompare = Math.min(tuple1.elementCount(), tuple2.elementCount());

        assert columnsToCompare <= numElements;

        for (int i = 0; i < columnsToCompare; i++) {
            Readability readability = tuple1.valueReadability(i);

            if (readability == Readability.NOT_READABLE) {
                return 0;
            }

            int res = compareField(i, tuple1, tuple2, readability);

            if (res != 0) {
                return res;
            }

            if (readability == Readability.PARTIAL_READABLE) {
                return 0;
            }
        }

        // We use the EQUALITY FLAG to determine the outcome of the comparison operation: if the flag is set, the prefix is considered
        // larger than the tuple and if the flag is not set, the prefix is considered smaller than the tuple. This is needed to include
        // or exclude the scan bounds.
        if (!isBuffer2Prefix) {
            return 0;
        } else {
            return -equalityFlag(buffer2);
        }
    }

    /**
     * Compares two tuples by column using given column index.
     */
    private int compareField(int colIdx, BinaryTupleReader tuple1, BinaryTupleReader tuple2, Readability readability) {
        assert readability != Readability.NOT_READABLE : "The field is run out of inline size and cannot be compared.";

        CatalogColumnCollation collation = columnCollations.get(colIdx);

        boolean tuple1HasNull = tuple1.hasNullValue(colIdx);
        boolean tuple2HasNull = tuple2.hasNullValue(colIdx);

        if (tuple1HasNull && tuple2HasNull) {
            return 0;
        } else if (tuple1HasNull || tuple2HasNull) {
            return collation.nullsFirst() == tuple1HasNull ? -1 : 1;
        }

        NativeType nativeType = columnTypes.get(colIdx);

        int res = readability == Readability.READABLE
                ? compareFieldValue(nativeType.spec(), tuple1, tuple2, colIdx)
                : compareFieldValuePartially(nativeType.spec(), tuple1, tuple2, colIdx);

        return collation.asc() ? res : -res;
    }

    private static int compareFieldValuePartially(
            ColumnType typeSpec,
            BinaryTupleReader partialTuple,
            BinaryTupleReader tuple2,
            int index
    ) {
        switch (typeSpec) {
            case BYTE_ARRAY:
                return compareAsBytes(partialTuple, tuple2, index);

            case UUID:
                return compareAsUuid(partialTuple, tuple2, index);

            case STRING:
                return compareAsString(partialTuple, tuple2, index);

            case TIMESTAMP:
                return compareAsTimestamp(partialTuple, tuple2, index);

            default:
                return 0;
        }
    }

    /**
     * Retrieves a trimmed portion of binary data from the specified tuple at a given index.
     * The size of the retrieved data is limited to the specified maximum length.
     *
     * @param tuple The BinaryTupleReader from which to retrieve the binary data.
     * @param index The index in the tuple specifying the location of the data to be retrieved.
     * @param maxLength The maximum allowable length for the trimmed portion of the binary data.
     * @return A byte array containing the trimmed binary data.
     */
    private static byte[] getTrimmedBytes(BinaryTupleReader tuple, int index, int maxLength) {
        tuple.seek(index);

        int begin = tuple.begin();
        int end = tuple.end();

        if (tuple.byteBuffer().get(begin) == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
            maxLength++;
        }

        int trimmedSize = Math.min(end - begin, maxLength);

        return tuple.bytesValue(begin, begin + trimmedSize);
    }
}
