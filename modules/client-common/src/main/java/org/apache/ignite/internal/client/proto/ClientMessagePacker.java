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

package org.apache.ignite.internal.client.proto;

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.HEADER_SIZE;
import static org.apache.ignite.internal.client.proto.ClientMessageCommon.NO_VALUE;
import static org.msgpack.core.MessagePack.Code;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;

/**
 * ByteBuf-based MsgPack implementation. Replaces {@link org.msgpack.core.MessagePacker} to avoid
 * extra buffers and indirection.
 *
 * <p>Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessagePacker implements AutoCloseable {
    /**
     * Underlying buffer.
     */
    private final ByteBuf buf;

    /**
     * Closed flag.
     */
    private boolean closed;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     */
    public ClientMessagePacker(ByteBuf buf) {
        this.buf = buf.writerIndex(HEADER_SIZE);
    }

    /**
     * Gets the underlying buffer, including 4-byte message length at the beginning.
     *
     * @return Underlying buffer.
     */
    public ByteBuf getBuffer() {
        buf.setInt(0, buf.writerIndex() - HEADER_SIZE);

        return buf;
    }

    /**
     * Writes a Nil value.
     */
    public void packNil() {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.NIL);
    }

    /**
     * Writes a "no value" value.
     */
    public void packNoValue() {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.FIXEXT1);
        buf.writeByte(ClientMsgPackType.NO_VALUE);
        buf.writeByte(0);
    }

    /**
     * Writes a boolean value.
     *
     * @param b the value to be written.
     */
    public void packBoolean(boolean b) {
        assert !closed : "Packer is closed";

        buf.writeByte(b ? Code.TRUE : Code.FALSE);
    }

    /**
     * Writes an Integer value.
     *
     * @param b the value to be written.
     */
    public void packByte(byte b) {
        assert !closed : "Packer is closed";

        if (b < -(1 << 5)) {
            buf.writeByte(Code.INT8);
        }

        buf.writeByte(b);
    }

    /**
     * Writes a short value.
     *
     * @param v the value to be written.
     */
    public void packShort(short v) {
        assert !closed : "Packer is closed";

        if (v < -(1 << 5)) {
            if (v < -(1 << 7)) {
                buf.writeByte(Code.INT16);
                buf.writeShort(v);
            } else {
                buf.writeByte(Code.INT8);
                buf.writeByte(v);
            }
        } else if (v < (1 << 7)) {
            buf.writeByte(v);
        } else {
            if (v < (1 << 8)) {
                buf.writeByte(Code.UINT8);
                buf.writeByte(v);
            } else {
                buf.writeByte(Code.UINT16);
                buf.writeShort(v);
            }
        }
    }

    /**
     * Writes an int value.
     *
     * @param i the value to be written.
     */
    public void packInt(int i) {
        assert !closed : "Packer is closed";

        if (i < -(1 << 5)) {
            if (i < -(1 << 15)) {
                buf.writeByte(Code.INT32);
                buf.writeInt(i);
            } else if (i < -(1 << 7)) {
                buf.writeByte(Code.INT16);
                buf.writeShort(i);
            } else {
                buf.writeByte(Code.INT8);
                buf.writeByte(i);
            }
        } else if (i < (1 << 7)) {
            // Includes negative fixint.
            buf.writeByte(i);
        } else {
            if (i < (1 << 8)) {
                buf.writeByte(Code.UINT8);
                buf.writeByte(i);
            } else if (i < (1 << 16)) {
                buf.writeByte(Code.UINT16);
                buf.writeShort(i);
            } else {
                buf.writeByte(Code.UINT32);
                buf.writeInt(i);
            }
        }
    }

    /**
     * Writes a long value.
     *
     * @param v the value to be written.
     */
    public void packLong(long v) {
        assert !closed : "Packer is closed";

        if (v < -(1L << 5)) {
            if (v < -(1L << 15)) {
                if (v < -(1L << 31)) {
                    buf.writeByte(Code.INT64);
                    buf.writeLong(v);
                } else {
                    buf.writeByte(Code.INT32);
                    buf.writeInt((int) v);
                }
            } else {
                if (v < -(1 << 7)) {
                    buf.writeByte(Code.INT16);
                    buf.writeShort((short) v);
                } else {
                    buf.writeByte(Code.INT8);
                    buf.writeByte((byte) v);
                }
            }
        } else if (v < (1 << 7)) {
            // fixnum
            buf.writeByte((byte) v);
        } else {
            if (v < (1L << 16)) {
                if (v < (1 << 8)) {
                    buf.writeByte(Code.UINT8);
                    buf.writeByte((byte) v);
                } else {
                    buf.writeByte(Code.UINT16);
                    buf.writeShort((short) v);
                }
            } else {
                if (v < (1L << 32)) {
                    buf.writeByte(Code.UINT32);
                    buf.writeInt((int) v);
                } else {
                    buf.writeByte(Code.UINT64);
                    buf.writeLong(v);
                }
            }
        }
    }

    /**
     * Writes a float value.
     *
     * @param v the value to be written.
     */
    public void packFloat(float v) {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.FLOAT32);
        buf.writeFloat(v);
    }

    /**
     * Writes a double value.
     *
     * @param v the value to be written.
     */
    public void packDouble(double v) {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.FLOAT64);
        buf.writeDouble(v);
    }

    /**
     * Writes a string value.
     *
     * @param s the value to be written.
     */
    public void packString(String s) {
        assert !closed : "Packer is closed";

        // Header is a varint.
        // Use pessimistic utf8MaxBytes to reserve bytes for the header.
        // This may cause an extra byte to be used for the header,
        // but this is cheaper than calculating correct utf8 byte size, which involves full string scan.
        int maxBytes = ByteBufUtil.utf8MaxBytes(s);
        int headerSize = getStringHeaderSize(maxBytes);
        int headerPos = buf.writerIndex();

        buf.writerIndex(headerPos + headerSize);

        int bytesWritten = ByteBufUtil.writeUtf8(buf, s);
        int endPos = buf.writerIndex();

        buf.writerIndex(headerPos);

        if (headerSize == 1) {
            buf.writeByte((byte) (Code.FIXSTR_PREFIX | bytesWritten));
        } else if (headerSize == 2) {
            buf.writeByte(Code.STR8);
            buf.writeByte(bytesWritten);
        } else if (headerSize == 3) {
            buf.writeByte(Code.STR16);
            buf.writeShort(bytesWritten);
        } else {
            assert headerSize == 5 : "headerSize == 5";
            buf.writeByte(Code.STR32);
            buf.writeInt(bytesWritten);
        }

        buf.writerIndex(endPos);
    }

    /**
     * Writes an array header value.
     *
     * @param arraySize array size.
     */
    public void packArrayHeader(int arraySize) {
        assert !closed : "Packer is closed";

        if (arraySize < 0) {
            throw new IllegalArgumentException("array size must be >= 0");
        }

        if (arraySize < (1 << 4)) {
            buf.writeByte((byte) (Code.FIXARRAY_PREFIX | arraySize));
        } else if (arraySize < (1 << 16)) {
            buf.writeByte(Code.ARRAY16);
            buf.writeShort(arraySize);
        } else {
            buf.writeByte(Code.ARRAY32);
            buf.writeInt(arraySize);
        }
    }

    /**
     * Writes a map header value.
     *
     * @param mapSize map size.
     */
    public void packMapHeader(int mapSize) {
        assert !closed : "Packer is closed";

        if (mapSize < 0) {
            throw new IllegalArgumentException("map size must be >= 0");
        }

        if (mapSize < (1 << 4)) {
            buf.writeByte((byte) (Code.FIXMAP_PREFIX | mapSize));
        } else if (mapSize < (1 << 16)) {
            buf.writeByte(Code.MAP16);
            buf.writeShort(mapSize);
        } else {
            buf.writeByte(Code.MAP32);
            buf.writeInt(mapSize);
        }
    }

    /**
     * Writes Extension value header.
     *
     * <p>Should be followed by {@link #writePayload(byte[])} method to write the extension body.
     *
     * @param extType    the extension type tag to be written.
     * @param payloadLen number of bytes of a payload binary to be written.
     */
    public void packExtensionTypeHeader(byte extType, int payloadLen) {
        assert !closed : "Packer is closed";

        if (payloadLen < (1 << 8)) {
            if (payloadLen > 0
                    && (payloadLen & (payloadLen - 1)) == 0) { // check whether dataLen == 2^x
                if (payloadLen == 1) {
                    buf.writeByte(Code.FIXEXT1);
                    buf.writeByte(extType);
                } else if (payloadLen == 2) {
                    buf.writeByte(Code.FIXEXT2);
                    buf.writeByte(extType);
                } else if (payloadLen == 4) {
                    buf.writeByte(Code.FIXEXT4);
                    buf.writeByte(extType);
                } else if (payloadLen == 8) {
                    buf.writeByte(Code.FIXEXT8);
                    buf.writeByte(extType);
                } else if (payloadLen == 16) {
                    buf.writeByte(Code.FIXEXT16);
                    buf.writeByte(extType);
                } else {
                    buf.writeByte(Code.EXT8);
                    buf.writeByte(payloadLen);
                    buf.writeByte(extType);
                }
            } else {
                buf.writeByte(Code.EXT8);
                buf.writeByte(payloadLen);
                buf.writeByte(extType);
            }
        } else if (payloadLen < (1 << 16)) {
            buf.writeByte(Code.EXT16);
            buf.writeShort(payloadLen);
            buf.writeByte(extType);
        } else {
            buf.writeByte(Code.EXT32);
            buf.writeInt(payloadLen);
            buf.writeByte(extType);
        }
    }

    /**
     * Writes a binary header value.
     *
     * @param len binary value size.
     */
    public void packBinaryHeader(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 8)) {
            buf.writeByte(Code.BIN8);
            buf.writeByte(len);
        } else if (len < (1 << 16)) {
            buf.writeByte(Code.BIN16);
            buf.writeShort(len);
        } else {
            buf.writeByte(Code.BIN32);
            buf.writeInt(len);
        }
    }

    /**
     * Writes a raw string header value.
     *
     * @param len string value size.
     */
    public void packRawStringHeader(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 5)) {
            buf.writeByte((byte) (Code.FIXSTR_PREFIX | len));
        } else if (len < (1 << 8)) {
            buf.writeByte(Code.STR8);
            buf.writeByte(len);
        } else if (len < (1 << 16)) {
            buf.writeByte(Code.STR16);
            buf.writeShort(len);
        } else {
            buf.writeByte(Code.STR32);
            buf.writeInt(len);
        }
    }

    /**
     * Writes a byte array to the output.
     *
     * <p>This method is used with {@link #packRawStringHeader(int)} or {@link #packBinaryHeader(int)}
     * methods.
     *
     * @param src the data to add.
     */
    public void writePayload(byte[] src) {
        assert !closed : "Packer is closed";

        buf.writeBytes(src);
    }

    /**
     * Writes a byte buffer to the output.
     *
     * <p>This method is used with {@link #packRawStringHeader(int)} or {@link #packBinaryHeader(int)}
     * methods.
     *
     * @param src the data to add.
     */
    public void writePayload(ByteBuffer src) {
        assert !closed : "Packer is closed";

        buf.writeBytes(src);
    }

    /**
     * Writes a byte array to the output.
     *
     * <p>This method is used with {@link #packRawStringHeader(int)} or {@link #packBinaryHeader(int)}
     * methods.
     *
     * @param src the data to add.
     * @param off the start offset in the data.
     * @param len the number of bytes to add.
     */
    public void writePayload(byte[] src, int off, int len) {
        assert !closed : "Packer is closed";

        buf.writeBytes(src, off, len);
    }

    /**
     * Writes a UUID.
     *
     * @param val UUID value.
     */
    public void packUuid(UUID val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.UUID, 16);

        buf.writeLong(val.getMostSignificantBits());
        buf.writeLong(val.getLeastSignificantBits());
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     */
    public void packDecimal(BigDecimal val) {
        assert !closed : "Packer is closed";

        byte[] unscaledValue = val.unscaledValue().toByteArray();

        // Pack scale as varint.
        int scale = val.scale();

        int scaleBytes = 5;

        if (scale < (1 << 7)) {
            scaleBytes = 1;
        } else if (scale < (1 << 8)) {
            scaleBytes = 2;
        } else if (scale < (1 << 16)) {
            scaleBytes = 3;
        }

        int payloadLen = scaleBytes + unscaledValue.length;

        packExtensionTypeHeader(ClientMsgPackType.DECIMAL, payloadLen);

        switch (scaleBytes) {
            case 1:
                buf.writeByte(scale);
                break;

            case 2:
                buf.writeByte(Code.UINT8);
                buf.writeByte(scale);
                break;

            case 3:
                buf.writeByte(Code.UINT16);
                buf.writeShort(scale);
                break;

            default:
                buf.writeByte(Code.UINT32);
                buf.writeInt(scale);
        }

        buf.writeBytes(unscaledValue);
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     */
    public void packNumber(BigInteger val) {
        assert !closed : "Packer is closed";

        byte[] data = val.toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.NUMBER, data.length);

        buf.writeBytes(data);
    }

    /**
     * Writes a bit set.
     *
     * @param val Bit set value.
     */
    public void packBitSet(BitSet val) {
        assert !closed : "Packer is closed";

        byte[] data = val.toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.BITMASK, data.length);

        buf.writeBytes(data);
    }

    /**
     * Writes an integer array.
     *
     * @param arr Integer array value.
     */
    public void packIntArray(int[] arr) {
        assert !closed : "Packer is closed";

        if (arr == null) {
            packNil();

            return;
        }

        packArrayHeader(arr.length);

        for (int i : arr) {
            packInt(i);
        }
    }

    /**
     * Writes a date.
     *
     * @param val Date value.
     */
    public void packDate(LocalDate val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.DATE, 6);

        buf.writeInt(val.getYear());
        buf.writeByte(val.getMonthValue());
        buf.writeByte(val.getDayOfMonth());
    }

    /**
     * Writes a time.
     *
     * @param val Time value.
     */
    public void packTime(LocalTime val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.TIME, 7);

        buf.writeByte(val.getHour());
        buf.writeByte(val.getMinute());
        buf.writeByte(val.getSecond());
        buf.writeInt(val.getNano());
    }

    /**
     * Writes a datetime.
     *
     * @param val Datetime value.
     */
    public void packDateTime(LocalDateTime val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.DATETIME, 13);

        buf.writeInt(val.getYear());
        buf.writeByte(val.getMonthValue());
        buf.writeByte(val.getDayOfMonth());
        buf.writeByte(val.getHour());
        buf.writeByte(val.getMinute());
        buf.writeByte(val.getSecond());
        buf.writeInt(val.getNano());
    }

    /**
     * Writes a timestamp.
     *
     * @param val Timestamp value.
     */
    public void packTimestamp(Instant val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.TIMESTAMP, 12);

        buf.writeLong(val.getEpochSecond());
        buf.writeInt(val.getNano());
    }

    /**
     * Packs an object.
     *
     * @param val Object value.
     * @throws UnsupportedOperationException When type is not supported.
     */
    public void packObject(Object val) {
        if (val == null) {
            packNil();

            return;
        }

        if (val == NO_VALUE) {
            packNoValue();

            return;
        }

        if (val instanceof Byte) {
            packByte((byte) val);

            return;
        }

        if (val instanceof Short) {
            packShort((short) val);

            return;
        }

        if (val instanceof Integer) {
            packInt((int) val);

            return;
        }

        if (val instanceof Long) {
            packLong((long) val);

            return;
        }

        if (val instanceof Float) {
            packFloat((float) val);

            return;
        }

        if (val instanceof Double) {
            packDouble((double) val);

            return;
        }

        if (val instanceof UUID) {
            packUuid((UUID) val);

            return;
        }

        if (val instanceof String) {
            packString((String) val);

            return;
        }

        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            packBinaryHeader(bytes.length);
            writePayload(bytes);

            return;
        }

        if (val instanceof BigDecimal) {
            packDecimal((BigDecimal) val);

            return;
        }

        if (val instanceof BigInteger) {
            packNumber((BigInteger) val);

            return;
        }

        if (val instanceof BitSet) {
            packBitSet((BitSet) val);

            return;
        }

        if (val instanceof LocalDate) {
            packDate((LocalDate) val);

            return;
        }

        if (val instanceof LocalTime) {
            packTime((LocalTime) val);

            return;
        }

        if (val instanceof LocalDateTime) {
            packDateTime((LocalDateTime) val);

            return;
        }

        if (val instanceof Instant) {
            packTimestamp((Instant) val);

            return;
        }

        throw new UnsupportedOperationException(
                "Unsupported type, can't serialize: " + val.getClass());
    }

    /**
     * Packs an array of objects in BinaryTuple format.
     *
     * @param vals Object array.
     */
    public void packObjectArrayAsBinaryTuple(Object[] vals) {
        assert !closed : "Packer is closed";

        if (vals == null) {
            packNil();

            return;
        }

        packInt(vals.length);

        // Builder with inline schema.
        // Every element in vals is represented by 3 tuple elements: type, scale, value.
        var builder = new BinaryTupleBuilder(vals.length * 3, true);

        for (Object arg : vals) {
            appendObjectToBinaryTuple(builder, arg);
        }

        packBinaryTuple(builder);
    }

    /**
     * Packs an objects in BinaryTuple format.
     *
     * @param val Object array.
     */
    public void packObjectAsBinaryTuple(Object val) {
        assert !closed : "Packer is closed";

        if (val == null) {
            packNil();

            return;
        }

        // Builder with inline schema.
        // Value is represented by 3 tuple elements: type, scale, value.
        var builder = new BinaryTupleBuilder(3, false, 3);
        appendObjectToBinaryTuple(builder, val);

        packBinaryTuple(builder);
    }

    private static void appendObjectToBinaryTuple(BinaryTupleBuilder builder, Object arg) {
        if (arg == null) {
            builder.appendNull();
            builder.appendInt(0);
            builder.appendNull();
        } else if (arg instanceof Byte) {
            builder.appendInt(ClientDataType.INT8);
            builder.appendInt(0);
            builder.appendByte((Byte) arg);
        } else if (arg instanceof Short) {
            builder.appendInt(ClientDataType.INT16);
            builder.appendInt(0);
            builder.appendShort((Short) arg);
        } else if (arg instanceof Integer) {
            builder.appendInt(ClientDataType.INT32);
            builder.appendInt(0);
            builder.appendInt((Integer) arg);
        } else if (arg instanceof Long) {
            builder.appendInt(ClientDataType.INT64);
            builder.appendInt(0);
            builder.appendLong((Long) arg);
        } else if (arg instanceof Float) {
            builder.appendInt(ClientDataType.FLOAT);
            builder.appendInt(0);
            builder.appendFloat((Float) arg);
        } else if (arg instanceof Double) {
            builder.appendInt(ClientDataType.DOUBLE);
            builder.appendInt(0);
            builder.appendDouble((Double) arg);
        } else if (arg instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) arg;

            builder.appendInt(ClientDataType.DECIMAL);
            builder.appendInt(bigDecimal.scale());
            builder.appendDecimal(bigDecimal, bigDecimal.scale());
        } else if (arg instanceof UUID) {
            builder.appendInt(ClientDataType.UUID);
            builder.appendInt(0);
            builder.appendUuid((UUID) arg);
        } else if (arg instanceof String) {
            builder.appendInt(ClientDataType.STRING);
            builder.appendInt(0);
            builder.appendString((String) arg);
        } else if (arg instanceof byte[]) {
            builder.appendInt(ClientDataType.BYTES);
            builder.appendInt(0);
            builder.appendBytes((byte[]) arg);
        } else if (arg instanceof BitSet) {
            builder.appendInt(ClientDataType.BITMASK);
            builder.appendInt(0);
            builder.appendBitmask((BitSet) arg);
        } else if (arg instanceof LocalDate) {
            builder.appendInt(ClientDataType.DATE);
            builder.appendInt(0);
            builder.appendDate((LocalDate) arg);
        } else if (arg instanceof LocalTime) {
            builder.appendInt(ClientDataType.TIME);
            builder.appendInt(0);
            builder.appendTime((LocalTime) arg);
        } else if (arg instanceof LocalDateTime) {
            builder.appendInt(ClientDataType.DATETIME);
            builder.appendInt(0);
            builder.appendDateTime((LocalDateTime) arg);
        } else if (arg instanceof Instant) {
            builder.appendInt(ClientDataType.TIMESTAMP);
            builder.appendInt(0);
            builder.appendTimestamp((Instant) arg);
        } else if (arg instanceof BigInteger) {
            builder.appendInt(ClientDataType.NUMBER);
            builder.appendInt(0);
            builder.appendNumber((BigInteger) arg);
        } else if (arg instanceof Boolean) {
            builder.appendInt(ClientDataType.BOOLEAN);
            builder.appendInt(0);
            builder.appendByte((byte) ((Boolean) arg ? 1 : 0));
        } else if (arg instanceof Duration) {
            builder.appendInt(ClientDataType.DURATION);
            builder.appendInt(0);
            builder.appendDuration((Duration) arg);
        } else if (arg instanceof Period) {
            builder.appendInt(ClientDataType.PERIOD);
            builder.appendInt(0);
            builder.appendPeriod((Period) arg);
        }
    }

    /**
     * Packs binary tuple with no-value set.
     *
     * @param builder Builder.
     * @param noValueSet No-value bit set.
     */
    public void packBinaryTuple(BinaryTupleBuilder builder, BitSet noValueSet) {
        packBitSet(noValueSet);

        // TODO IGNITE-17821 Thin 3.0 Perf: Implement BinaryTupleReader and Builder over ByteBuf.
        var buf = builder.build();
        packBinaryHeader(buf.limit() - buf.position());
        writePayload(buf);
    }

    /**
     * Packs binary tuple.
     *
     * @param builder Builder.
     */
    public void packBinaryTuple(BinaryTupleBuilder builder) {
        var buf = builder.build();
        packBinaryHeader(buf.limit() - buf.position());
        writePayload(buf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        if (buf.refCnt() > 0) {
            buf.release();
        }
    }

    /**
     * Gets the varint string header size in bytes.
     *
     * @param len String length.
     * @return String header size, in bytes.
     */
    private int getStringHeaderSize(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 5)) {
            return 1;
        }

        if (len < (1 << 8)) {
            return 2;
        }

        if (len < (1 << 16)) {
            return 3;
        }

        return 5;
    }
}
