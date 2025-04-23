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

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.EQUALITY_FLAG;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.type.NativeTypeSpec;

/**
 * The utility class has methods to use to compare fields in binary representation.
 */
class BinaryTupleComparatorUtils {
    /**
     * Compares individual fields of two tuples using ascending order.
     */
    @SuppressWarnings("DataFlowIssue")
    static int compareFieldValue(NativeTypeSpec typeSpec, BinaryTupleReader tuple1, int index1, BinaryTupleReader tuple2, int index2) {
        switch (typeSpec) {
            case INT8:
            case BOOLEAN:
                return Byte.compare(tuple1.byteValue(index1), tuple2.byteValue(index2));

            case INT16:
                return Short.compare(tuple1.shortValue(index1), tuple2.shortValue(index2));

            case INT32:
                return Integer.compare(tuple1.intValue(index1), tuple2.intValue(index2));

            case INT64:
                return Long.compare(tuple1.longValue(index1), tuple2.longValue(index2));

            case FLOAT:
                return Float.compare(tuple1.floatValue(index1), tuple2.floatValue(index2));

            case DOUBLE:
                return Double.compare(tuple1.doubleValue(index1), tuple2.doubleValue(index2));

            case BYTES:
                return Arrays.compareUnsigned(tuple1.bytesValue(index1), tuple2.bytesValue(index2));

            case UUID:
                return tuple1.uuidValue(index1).compareTo(tuple2.uuidValue(index2));

            case STRING:
                return tuple1.stringValue(index1).compareTo(tuple2.stringValue(index2));

            case DECIMAL:
                BigDecimal numeric1 = tuple1.decimalValue(index1, Integer.MIN_VALUE);
                BigDecimal numeric2 = tuple2.decimalValue(index2, Integer.MIN_VALUE);

                return numeric1.compareTo(numeric2);

            case TIMESTAMP:
                return tuple1.timestampValue(index1).compareTo(tuple2.timestampValue(index2));

            case DATE:
                return tuple1.dateValue(index1).compareTo(tuple2.dateValue(index2));

            case TIME:
                return tuple1.timeValue(index1).compareTo(tuple2.timeValue(index2));

            case DATETIME:
                return tuple1.dateTimeValue(index1).compareTo(tuple2.dateTimeValue(index2));

            default:
                throw new IllegalArgumentException(format("Unsupported column type in binary tuple comparator. [type={}]", typeSpec));
        }
    }

    static boolean isFlagSet(ByteBuffer tuple, int flag) {
        return (tuple.get(0) & flag) != 0;
    }

    static int equalityFlag(ByteBuffer tuple) {
        return isFlagSet(tuple, EQUALITY_FLAG) ? 1 : -1;
    }
}
