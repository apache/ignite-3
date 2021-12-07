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
    void appendNull();

    void appendByte(byte val);

    void appendShort(short val);

    void appendInt(int val);

    void appendLong(long val);

    void appendFloat(float val);

    void appendDouble(double val);

    void appendString(String val);

    void appendUuid(UUID val);

    void appendBytes(byte[] val);

    void appendBitmask(BitSet val);

    void appendNumber(BigInteger val);

    void appendDecimal(BigDecimal val);

    void appendDate(LocalDate val);

    void appendTime(LocalTime val);

    void appendTimestamp(Instant val);

    void appendDateTime(LocalDateTime val);
    
    default void writeValue(MarshallerColumn col, Object val) {
        if (val == null) {
            appendNull();

            return;
        }

        switch (col.type()) {
            case BYTE: {
                appendByte((byte) val);

                break;
            }
            case SHORT: {
                appendShort((short) val);

                break;
            }
            case INT: {
                appendInt((int) val);

                break;
            }
            case LONG: {
                appendLong((long) val);

                break;
            }
            case FLOAT: {
                appendFloat((float) val);

                break;
            }
            case DOUBLE: {
                appendDouble((double) val);

                break;
            }
            case UUID: {
                appendUuid((UUID) val);

                break;
            }
            case TIME: {
                appendTime((LocalTime) val);

                break;
            }
            case DATE: {
                appendDate((LocalDate) val);

                break;
            }
            case DATETIME: {
                appendDateTime((LocalDateTime) val);

                break;
            }
            case TIMESTAMP: {
                appendTimestamp((Instant) val);

                break;
            }
            case STRING: {
                appendString((String) val);

                break;
            }
            case BYTE_ARR: {
                appendBytes((byte[]) val);

                break;
            }
            case BITSET: {
                appendBitmask((BitSet) val);

                break;
            }
            case NUMBER: {
                appendNumber((BigInteger) val);

                break;
            }
            case DECIMAL: {
                appendDecimal((BigDecimal) val);

                break;
            }
            default:
                throw new IgniteException("Unexpected value: " + col.type());
        }
    }
}
