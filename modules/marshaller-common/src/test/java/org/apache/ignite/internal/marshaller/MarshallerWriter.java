/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.lang.IgniteException;

public interface MarshallerWriter {
    void writeNull();

    void writeByte(byte val);

    void writeShort(short val);

    void writeInt(int val);

    void writeLong(long val);

    void writeFloat(float val);

    void writeDouble(double val);

    void writeString(String val);

    void writeUuid(UUID val);

    void writeBytes(byte[] val);

    void writeBitSet(BitSet val);

    void writeBigInt(BigInteger val);

    void writeBigDecimal(BigDecimal val);

    void writeDate(LocalDate val);

    void writeTime(LocalTime val);

    void writeTimestamp(Instant val);

    void writeDateTime(LocalDateTime val);
    
    default void writeValue(MarshallerColumn col, Object val) {
        if (val == null) {
            writeNull();

            return;
        }

        switch (col.type()) {
            case BYTE: {
                writeByte((byte) val);

                break;
            }
            case SHORT: {
                writeShort((short) val);

                break;
            }
            case INT: {
                writeInt((int) val);

                break;
            }
            case LONG: {
                writeLong((long) val);

                break;
            }
            case FLOAT: {
                writeFloat((float) val);

                break;
            }
            case DOUBLE: {
                writeDouble((double) val);

                break;
            }
            case UUID: {
                writeUuid((UUID) val);

                break;
            }
            case TIME: {
                writeTime((LocalTime) val);

                break;
            }
            case DATE: {
                writeDate((LocalDate) val);

                break;
            }
            case DATETIME: {
                writeDateTime((LocalDateTime) val);

                break;
            }
            case TIMESTAMP: {
                writeTimestamp((Instant) val);

                break;
            }
            case STRING: {
                writeString((String) val);

                break;
            }
            case BYTE_ARR: {
                writeBytes((byte[]) val);

                break;
            }
            case BITSET: {
                writeBitSet((BitSet) val);

                break;
            }
            case NUMBER: {
                writeBigInt((BigInteger) val);

                break;
            }
            case DECIMAL: {
                writeBigDecimal((BigDecimal) val);

                break;
            }
            default:
                throw new IgniteException("Unexpected value: " + col.type());
        }
    }
}
