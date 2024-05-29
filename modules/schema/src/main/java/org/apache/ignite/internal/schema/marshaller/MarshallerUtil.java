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

package org.apache.ignite.internal.schema.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.marshaller.BinaryMode;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.util.ObjectFactory;

/**
 * Marshaller utility class.
 */
public final class MarshallerUtil {
    /**
     * Calculates size for serialized value of varlen type.
     *
     * @param val  Field value.
     * @param type Mapped type.
     * @return Serialized value size.
     * @throws InvalidTypeException If type is unsupported.
     */
    public static int getValueSize(Object val, NativeType type) throws InvalidTypeException {
        switch (type.spec()) {
            case BYTES:
                if (val instanceof byte[]) {
                    byte[] bytes = (byte[]) val;
                    if (bytes.length == 0 || bytes[0] == BinaryTupleCommon.VARLEN_EMPTY_BYTE) {
                        return bytes.length + 1;
                    }
                    return bytes.length;
                }
                // Return zero for pojo as they are not serialized yet.
                return 0;

            case STRING:
                CharSequence chars = (CharSequence) val;
                return chars.length() == 0 ? 1 : utf8EncodedLength(chars);

            case NUMBER:
                return sizeInBytes((BigInteger) val);

            case DECIMAL:
                return sizeInBytes((BigDecimal) val);

            default:
                throw new InvalidTypeException("Unsupported variable-length type: " + type);
        }
    }

    /**
     * Converts the passed value to a more compact form, if possible.
     *
     * @param value Field value.
     * @param type Mapped type.
     * @return Value in a more compact form, or the original value if it cannot be compacted.
     */
    public static Object shrinkValue(Object value, NativeType type) {
        if (type.spec() == NativeTypeSpec.DECIMAL) {
            assert type instanceof DecimalNativeType;

            return BinaryTupleCommon.shrinkDecimal((BigDecimal) value, ((DecimalNativeType) type).scale());
        }

        return value;
    }

    /**
     * Creates object factory for class.
     *
     * @param targetCls Target class.
     * @param <T>       Target type.
     * @return Object factory.
     */
    public static <T> ObjectFactory<T> factoryForClass(Class<T> targetCls) {
        if (BinaryMode.forClass(targetCls) == BinaryMode.POJO) {
            return new ObjectFactory<>(targetCls);
        } else {
            return null;
        }
    }

    /**
     * Stub.
     */
    private MarshallerUtil() {
    }

    /**
     * Calculates encoded string length.
     *
     * @param seq Char sequence.
     * @return Encoded string length.
     * @implNote This implementation is not tolerant to malformed char sequences.
     */
    public static int utf8EncodedLength(CharSequence seq) {
        int cnt = 0;

        for (int i = 0, len = seq.length(); i < len; i++) {
            char ch = seq.charAt(i);

            if (ch <= 0x7F) {
                cnt++;
            } else if (ch <= 0x7FF) {
                cnt += 2;
            } else if (Character.isHighSurrogate(ch)) {
                cnt += 4;
                ++i;
            } else {
                cnt += 3;
            }
        }

        return cnt;
    }

    /**
     * Calculates byte size for BigInteger value.
     */
    public static int sizeInBytes(BigInteger val) {
        return val.bitLength() / 8 + 1;
    }

    /**
     * Calculates byte size for BigDecimal value.
     */
    public static int sizeInBytes(BigDecimal val) {
        return sizeInBytes(val.unscaledValue()) + Short.BYTES /* Size of scale */;
    }

    /**
     * Converts a given {@link Column} into a {@link MarshallerColumn}.
     */
    public static MarshallerColumn toMarshallerColumn(Column column) {
        NativeType columnType = column.type();

        return new MarshallerColumn(
                column.positionInRow(),
                column.name(),
                mode(columnType),
                column.defaultValueProvider()::get,
                columnType instanceof DecimalNativeType ? ((DecimalNativeType) columnType).scale() : 0
        );
    }

    /**
     * Converts an array of {@link Column}s into an array of {@link MarshallerColumn}s.
     */
    public static MarshallerColumn[] toMarshallerColumns(List<Column> columns) {
        var result = new MarshallerColumn[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            result[i] = toMarshallerColumn(columns.get(i));
        }

        return result;
    }

    /**
     * Converts a given {@link NativeType} into a {@link BinaryMode}.
     */
    private static BinaryMode mode(NativeType type) {
        switch (type.spec()) {
            case INT8:
                return BinaryMode.BYTE;
            case INT16:
                return BinaryMode.SHORT;
            case INT32:
                return BinaryMode.INT;
            case INT64:
                return BinaryMode.LONG;
            case FLOAT:
                return BinaryMode.FLOAT;
            case DOUBLE:
                return BinaryMode.DOUBLE;
            case DECIMAL:
                return BinaryMode.DECIMAL;
            case UUID:
                return BinaryMode.UUID;
            case STRING:
                return BinaryMode.STRING;
            case BYTES:
                return BinaryMode.BYTE_ARR;
            case BITMASK:
                return BinaryMode.BITSET;
            case NUMBER:
                return BinaryMode.NUMBER;
            case DATE:
                return BinaryMode.DATE;
            case TIME:
                return BinaryMode.TIME;
            case DATETIME:
                return BinaryMode.DATETIME;
            case TIMESTAMP:
                return BinaryMode.TIMESTAMP;
            case BOOLEAN:
                return BinaryMode.BOOLEAN;
            default:
                throw new IllegalArgumentException("No matching mode for type " + type);
        }
    }
}
