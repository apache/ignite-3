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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
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
     * Gets binary read/write mode for given class.
     *
     * @param cls Type.
     * @return Binary mode.
     */
    public static BinaryMode mode(Class<?> cls) {
        // Primitives.
        if (cls == boolean.class) {
            return BinaryMode.P_BOOLEAN;
        } else if (cls == byte.class) {
            return BinaryMode.P_BYTE;
        } else if (cls == short.class) {
            return BinaryMode.P_SHORT;
        } else if (cls == int.class) {
            return BinaryMode.P_INT;
        } else if (cls == long.class) {
            return BinaryMode.P_LONG;
        } else if (cls == float.class) {
            return BinaryMode.P_FLOAT;
        } else if (cls == double.class) {
            return BinaryMode.P_DOUBLE;
        } else if (cls == Boolean.class) { // Boxed primitives.
            return BinaryMode.BOOLEAN;
        } else if (cls == Byte.class) {
            return BinaryMode.BYTE;
        } else if (cls == Short.class) {
            return BinaryMode.SHORT;
        } else if (cls == Integer.class) {
            return BinaryMode.INT;
        } else if (cls == Long.class) {
            return BinaryMode.LONG;
        } else if (cls == Float.class) {
            return BinaryMode.FLOAT;
        } else if (cls == Double.class) {
            return BinaryMode.DOUBLE;
        } else if (cls == LocalDate.class) { // Temporal types
            return BinaryMode.DATE;
        } else if (cls == LocalTime.class) {
            return BinaryMode.TIME;
        } else if (cls == LocalDateTime.class) {
            return BinaryMode.DATETIME;
        } else if (cls == Instant.class) {
            return BinaryMode.TIMESTAMP;
        } else if (cls == byte[].class) { // Other types
            return BinaryMode.BYTE_ARR;
        } else if (cls == String.class) {
            return BinaryMode.STRING;
        } else if (cls == UUID.class) {
            return BinaryMode.UUID;
        } else if (cls == BitSet.class) {
            return BinaryMode.BITSET;
        } else if (cls == BigInteger.class) {
            return BinaryMode.NUMBER;
        } else if (cls == BigDecimal.class) {
            return BinaryMode.DECIMAL;
        }

        return BinaryMode.POJO;
    }

    /**
     * Creates object factory for class.
     *
     * @param targetCls Target class.
     * @param <T>       Target type.
     * @return Object factory.
     */
    public static <T> ObjectFactory<T> factoryForClass(Class<T> targetCls) {
        if (mode(targetCls) == BinaryMode.POJO) {
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
        return sizeInBytes(val.unscaledValue());
    }
}
