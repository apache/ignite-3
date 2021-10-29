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

package org.apache.ignite.internal.client.proto;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import org.apache.ignite.lang.IgniteUuid;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.OutputStreamBufferOutput;
import org.msgpack.value.Value;

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.HEADER_SIZE;
import static org.msgpack.core.MessagePack.Code;

/**
 * Ignite-specific MsgPack extension based on Netty ByteBuf.
 * <p>
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessagePacker extends MessagePacker {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Closed flag. */
    private boolean closed;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     */
    public ClientMessagePacker(ByteBuf buf) {
        // TODO: Remove intermediate classes and buffers IGNITE-15234.
        // TODO: Make all methods void?
        // TODO: Replace inheritdoc.
        // Reserve 4 bytes for the message length.
        super(new OutputStreamBufferOutput(new ByteBufOutputStream(buf.writerIndex(HEADER_SIZE))),
                MessagePack.DEFAULT_PACKER_CONFIG);

        this.buf = buf;
    }

    /**
     * Gets the underlying buffer.
     *
     * @return Underlying buffer.
     * @throws UncheckedIOException When flush fails.
     */
    public ByteBuf getBuffer() {
        try {
            flush();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        buf.setInt(0, buf.writerIndex() - HEADER_SIZE);

        return buf;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packNil() {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.NIL);

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packBoolean(boolean b) {
        assert !closed : "Packer is closed";

        buf.writeByte(b ? Code.TRUE : Code.FALSE);

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packByte(byte b) {
        assert !closed : "Packer is closed";

        if (b < -(1 << 5))
            buf.writeByte(Code.INT8);

        buf.writeByte(b);

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packShort(short v) {
        assert !closed : "Packer is closed";

        if (v < -(1 << 5)) {
            if (v < -(1 << 7)) {
                buf.writeByte(Code.INT16);
                buf.writeShort(v);
            }
            else {
                buf.writeByte(Code.INT8);
                buf.writeByte(v);
            }
        }
        else if (v < (1 << 7)) {
            buf.writeByte(v);
        }
        else {
            if (v < (1 << 8)) {
                buf.writeByte(Code.UINT8);
                buf.writeByte(v);
            }
            else {
                buf.writeByte(Code.UINT16);
                buf.writeShort(v);
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packInt(int r) {
        assert !closed : "Packer is closed";

        if (r < -(1 << 5)) {
            if (r < -(1 << 15)) {
                buf.writeByte(Code.INT32);
                buf.writeInt(r);
            }
            else if (r < -(1 << 7)) {
                buf.writeByte(Code.INT16);
                buf.writeShort(r);
            }
            else {
                buf.writeByte(Code.INT8);
                buf.writeByte(r);
            }
        }
        else if (r < (1 << 7)) {
            buf.writeByte(r);
        }
        else {
            if (r < (1 << 8)) {
                buf.writeByte(Code.UINT8);
                buf.writeByte(r);
            }
            else if (r < (1 << 16)) {
                buf.writeByte(Code.UINT16);
                buf.writeShort(r);
            }
            else {
                buf.writeByte(Code.UINT32);
                buf.writeInt(r);
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packLong(long v) {
        assert !closed : "Packer is closed";

        if (v < -(1L << 5)) {
            if (v < -(1L << 15)) {
                if (v < -(1L << 31)) {
                    buf.writeByte(Code.INT64);
                    buf.writeLong(v);
                }
                else {
                    buf.writeByte(Code.INT32);
                    buf.writeInt((int) v);
                }
            }
            else {
                if (v < -(1 << 7)) {
                    buf.writeByte(Code.INT16);
                    buf.writeShort((short) v);
                } else {
                    buf.writeByte(Code.INT8);
                    buf.writeByte((byte) v);
                }
            }
        }
        else if (v < (1 << 7)) {
            // fixnum
            buf.writeByte((byte)v);
        }
        else {
            if (v < (1L << 16)) {
                if (v < (1 << 8)) {
                    buf.writeByte(Code.UINT8);
                    buf.writeByte((byte) v);
                }
                else {
                    buf.writeByte(Code.UINT16);
                    buf.writeShort((short) v);
                }
            }
            else {
                if (v < (1L << 32)) {
                    buf.writeByte(Code.UINT32);
                    buf.writeInt((int) v);
                }
                else {
                    buf.writeByte(Code.UINT64);
                    buf.writeLong(v);
                }
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packBigInteger(BigInteger bi) {
        assert !closed : "Packer is closed";

        if (bi.bitLength() <= 63) {
            packLong(bi.longValue());
        }
        else if (bi.bitLength() == 64 && bi.signum() == 1) {
            buf.writeByte(Code.UINT64);
            buf.writeLong(bi.longValue());
        }
        else {
            throw new IllegalArgumentException("MessagePack cannot serialize BigInteger larger than 2^64-1");
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packFloat(float v) {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.FLOAT32);
        buf.writeFloat(v);

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packDouble(double v) {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.FLOAT64);
        buf.writeDouble(v);

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packString(String s) {
        assert !closed : "Packer is closed";

        // Header is a varint.
        // Use pessimistic utf8MaxBytes to reserve bytes for the header.
        int maxBytes = ByteBufUtil.utf8MaxBytes(s);
        int headerSize = getStringHeaderSize(maxBytes);
        int headerPos = buf.writerIndex();

        buf.writerIndex(headerPos + headerSize);

        int bytesWritten = ByteBufUtil.writeUtf8(buf, s);
        int endPos = buf.writerIndex();

        buf.writerIndex(headerPos);

        if (headerSize == 1)
            buf.writeByte((byte) (Code.FIXSTR_PREFIX | bytesWritten));
        else if (headerSize == 2) {
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

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packArrayHeader(int arraySize) {
        assert !closed : "Packer is closed";

        if (arraySize < 0) {
            throw new IllegalArgumentException("array size must be >= 0");
        }

        if (arraySize < (1 << 4)) {
            buf.writeByte((byte) (Code.FIXARRAY_PREFIX | arraySize));
        }
        else if (arraySize < (1 << 16)) {
            buf.writeByte(Code.ARRAY16);
            buf.writeShort(arraySize);
        }
        else {
            buf.writeByte(Code.ARRAY32);
            buf.writeInt(arraySize);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packMapHeader(int mapSize) {
        assert !closed : "Packer is closed";

        if (mapSize < 0) {
            throw new IllegalArgumentException("map size must be >= 0");
        }

        if (mapSize < (1 << 4)) {
            buf.writeByte((byte) (Code.FIXMAP_PREFIX | mapSize));
        }
        else if (mapSize < (1 << 16)) {
            buf.writeByte(Code.MAP16);
            buf.writeShort(mapSize);
        }
        else {
            buf.writeByte(Code.MAP32);
            buf.writeInt(mapSize);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packValue(Value v) {
        throw new UnsupportedOperationException("TODO: Remove");
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packExtensionTypeHeader(byte extType, int payloadLen) {
        assert !closed : "Packer is closed";

        if (payloadLen < (1 << 8)) {
            if (payloadLen > 0 && (payloadLen & (payloadLen - 1)) == 0) { // check whether dataLen == 2^x
                if (payloadLen == 1) {
                    buf.writeByte(Code.FIXEXT1);
                    buf.writeByte(extType);
                }
                else if (payloadLen == 2) {
                    buf.writeByte(Code.FIXEXT2);
                    buf.writeByte(extType);
                }
                else if (payloadLen == 4) {
                    buf.writeByte(Code.FIXEXT4);
                    buf.writeByte(extType);
                }
                else if (payloadLen == 8) {
                    buf.writeByte(Code.FIXEXT8);
                    buf.writeByte(extType);
                }
                else if (payloadLen == 16) {
                    buf.writeByte(Code.FIXEXT16);
                    buf.writeByte(extType);
                }
                else {
                    buf.writeByte(Code.EXT8);
                    buf.writeByte(payloadLen);
                    buf.writeByte(extType);
                }
            }
            else {
                buf.writeByte(Code.EXT8);
                buf.writeByte(payloadLen);
                buf.writeByte(extType);
            }
        }
        else if (payloadLen < (1 << 16)) {
            buf.writeByte(Code.EXT16);
            buf.writeShort(payloadLen);
            buf.writeByte(extType);
        }
        else {
            buf.writeByte(Code.EXT32);
            buf.writeInt(payloadLen);
            buf.writeByte(extType);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packBinaryHeader(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 8)) {
            buf.writeByte(Code.BIN8);
            buf.writeByte(len);
        }
        else if (len < (1 << 16)) {
            buf.writeByte(Code.BIN16);
            buf.writeShort(len);
        }
        else {
            buf.writeByte(Code.BIN32);
            buf.writeInt(len);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker packRawStringHeader(int len) {
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

        return this;
    }

    private int getStringHeaderSize(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 5))
            return 1;

        if (len < (1 << 8))
            return 2;

        if (len < (1 << 16))
            return 3;

        return 5;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker writePayload(byte[] src) {
        assert !closed : "Packer is closed";

        buf.writeBytes(src);

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker writePayload(byte[] src, int off, int len) {
        assert !closed : "Packer is closed";

        buf.writeBytes(src, off, len);

        return this;
    }

    /** {@inheritDoc} */
    @Override public MessagePacker addPayload(byte[] src) {
        assert !closed : "Packer is closed";

        try {
            return super.addPayload(src);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessagePacker addPayload(byte[] src, int off, int len) {
        assert !closed : "Packer is closed";

        try {
            return super.addPayload(src, off, len);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes an UUID.
     *
     * @param val UUID value.
     * @return This instance.
     */
    public ClientMessagePacker packUuid(UUID val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.UUID, 16);

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        var bytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(bytes);

        bb.putLong(val.getMostSignificantBits());
        bb.putLong(val.getLeastSignificantBits());

        addPayload(bytes);

        return this;
    }

    /**
     * Writes an {@link IgniteUuid}.
     *
     * @param val {@link IgniteUuid} value.
     * @return This instance.
     */
    public ClientMessagePacker packIgniteUuid(IgniteUuid val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.IGNITE_UUID, 24);

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        var bytes = new byte[24];
        ByteBuffer bb = ByteBuffer.wrap(bytes);

        UUID globalId = val.globalId();

        bb.putLong(globalId.getMostSignificantBits());
        bb.putLong(globalId.getLeastSignificantBits());

        bb.putLong(val.localId());

        writePayload(bytes);

        return this;
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     * @return This instance.
     */
    public ClientMessagePacker packDecimal(BigDecimal val) {
        assert !closed : "Packer is closed";

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        byte[] unscaledValue = val.unscaledValue().toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.DECIMAL, 4 + unscaledValue.length); // Scale length + data length

        addPayload(ByteBuffer.wrap(new byte[4]).putInt(val.scale()).array());
        addPayload(unscaledValue);

        return this;
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     * @return This instance.
     */
    public ClientMessagePacker packNumber(BigInteger val) {
        assert !closed : "Packer is closed";

        byte[] data = val.toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.NUMBER, data.length);

        addPayload(data);

        return this;
    }

    /**
     * Writes a bit set.
     *
     * @param val Bit set value.
     * @return This instance.
     */
    public ClientMessagePacker packBitSet(BitSet val) {
        assert !closed : "Packer is closed";

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        byte[] data = val.toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.BITMASK, data.length);

        addPayload(data);

        return this;
    }

    /**
     * Writes an integer array.
     *
     * @param arr Integer array value.
     * @return This instance.
     */
    public ClientMessagePacker packIntArray(int[] arr) {
        assert !closed : "Packer is closed";

        if (arr == null) {
            packNil();

            return this;
        }

        packArrayHeader(arr.length);

        for (int i : arr)
            packInt(i);

        return this;
    }

    /**
     * Writes a date.
     *
     * @param val Date value.
     * @return This instance.
     */
    public ClientMessagePacker packDate(LocalDate val) {
        assert !closed : "Packer is closed";

        byte[] data = new byte[6];

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        ByteBuffer.wrap(data)
            .putInt(val.getYear())
            .put((byte)val.getMonthValue())
            .put((byte)val.getDayOfMonth());

        packExtensionTypeHeader(ClientMsgPackType.DATE, data.length);

        addPayload(data);

        return this;
    }

    /**
     * Writes a time.
     *
     * @param val Time value.
     * @return This instance.
     */
    public ClientMessagePacker packTime(LocalTime val) {
        assert !closed : "Packer is closed";

        byte[] data = new byte[7];

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        ByteBuffer.wrap(data)
            .put((byte)val.getHour())
            .put((byte)val.getMinute())
            .put((byte)val.getSecond())
            .putInt(val.getNano());

        packExtensionTypeHeader(ClientMsgPackType.TIME, data.length);

        addPayload(data);

        return this;
    }

    /**
     * Writes a datetime.
     *
     * @param val Datetime value.
     * @return This instance.
     */
    public ClientMessagePacker packDateTime(LocalDateTime val) {
        assert !closed : "Packer is closed";

        byte[] data = new byte[13];

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        ByteBuffer.wrap(data)
            .putInt(val.getYear())
            .put((byte)val.getMonthValue())
            .put((byte)val.getDayOfMonth())
            .put((byte)val.getHour())
            .put((byte)val.getMinute())
            .put((byte)val.getSecond())
            .putInt(val.getNano());

        packExtensionTypeHeader(ClientMsgPackType.DATETIME, data.length);

        addPayload(data);

        return this;
    }

    /**
     * Writes a timestamp.
     *
     * @param val Timestamp value.
     * @return This instance.
     * @throws UnsupportedOperationException Not supported.
     */
    public ClientMessagePacker packTimestamp(Instant val) {
        assert !closed : "Packer is closed";

        byte[] data = new byte[12];

        // TODO: Pack directly to ByteBuf without allocating IGNITE-15234.
        ByteBuffer.wrap(data)
            .putLong(val.getEpochSecond())
            .putInt(val.getNano());

        packExtensionTypeHeader(ClientMsgPackType.TIMESTAMP, data.length);

        addPayload(data);

        return this;
    }

    /**
     * Packs an object.
     *
     * @param val Object value.
     * @return This instance.
     * @throws UnsupportedOperationException When type is not supported.
     */
    public ClientMessagePacker packObject(Object val) {
        if (val == null)
            return (ClientMessagePacker)packNil();

        if (val instanceof Byte)
            return (ClientMessagePacker)packByte((byte)val);

        if (val instanceof Short)
            return (ClientMessagePacker)packShort((short)val);

        if (val instanceof Integer)
            return (ClientMessagePacker)packInt((int)val);

        if (val instanceof Long)
            return (ClientMessagePacker)packLong((long)val);

        if (val instanceof Float)
            return (ClientMessagePacker)packFloat((float)val);

        if (val instanceof Double)
            return (ClientMessagePacker)packDouble((double)val);

        if (val instanceof UUID)
            return packUuid((UUID)val);

        if (val instanceof String)
            return (ClientMessagePacker)packString((String)val);

        if (val instanceof byte[]) {
            byte[] bytes = (byte[])val;
            packBinaryHeader(bytes.length);
            writePayload(bytes);

            return this;
        }

        if (val instanceof BigDecimal)
            return packDecimal((BigDecimal)val);

        if (val instanceof BigInteger)
            return packNumber((BigInteger)val);

        if (val instanceof BitSet)
            return packBitSet((BitSet)val);

        if (val instanceof LocalDate)
            return packDate((LocalDate)val);

        if (val instanceof LocalTime)
            return packTime((LocalTime)val);

        if (val instanceof LocalDateTime)
            return packDateTime((LocalDateTime)val);

        if (val instanceof Instant)
            return packTimestamp((Instant)val);

        throw new UnsupportedOperationException("Unsupported type, can't serialize: " + val.getClass());
    }

    /**
     * Packs an array of different objects.
     *
     * @param args Object array.
     * @return This instance.
     * @throws UnsupportedOperationException in case of unknown type.
     */
    public ClientMessagePacker packObjectArray(Object[] args) {
        assert !closed : "Packer is closed";

        if (args == null) {
            packNil();

            return this;
        }

        packArrayHeader(args.length);

        for (Object arg : args) {
            if (arg == null) {
                packNil();

                continue;
            }

            Class<?> cls = arg.getClass();

            if (cls == Boolean.class) {
                packInt(ClientDataType.BOOLEAN);
                packBoolean((Boolean)arg);
            }
            else if (cls == Byte.class) {
                packInt(ClientDataType.INT8);
                packByte((Byte)arg);
            }
            else if (cls == Short.class) {
                packInt(ClientDataType.INT16);
                packShort((Short)arg);
            }
            else if (cls == Integer.class) {
                packInt(ClientDataType.INT32);
                packInt((Integer)arg);
            }
            else if (cls == Long.class) {
                packInt(ClientDataType.INT64);
                packLong((Long)arg);
            }
            else if (cls == Float.class) {
                packInt(ClientDataType.FLOAT);
                packFloat((Float)arg);
            }
            else if (cls == Double.class) {
                packInt(ClientDataType.DOUBLE);
                packDouble((Double)arg);
            }
            else if (cls == String.class) {
                packInt(ClientDataType.STRING);
                packString((String)arg);
            }
            else if (cls == UUID.class) {
                packInt(ClientDataType.UUID);
                packUuid((UUID)arg);
            }
            else if (cls == LocalDate.class) {
                packInt(ClientDataType.DATE);
                packDate((LocalDate)arg);
            }
            else if (cls == LocalTime.class) {
                packInt(ClientDataType.TIME);
                packTime((LocalTime)arg);
            }
            else if (cls == LocalDateTime.class) {
                packInt(ClientDataType.DATETIME);
                packDateTime((LocalDateTime)arg);
            }
            else if (cls == Instant.class) {
                packInt(ClientDataType.TIMESTAMP);
                packTimestamp((Instant)arg);
            }
            else if (cls == byte[].class) {
                packInt(ClientDataType.BYTES);

                packBinaryHeader(((byte[])arg).length);
                writePayload((byte[])arg);
            }
            else if (cls == Date.class) {
                packInt(ClientDataType.DATE);
                packDate(((Date)arg).toLocalDate());
            }
            else if (cls == Time.class) {
                packInt(ClientDataType.TIME);
                packTime(((Time)arg).toLocalTime());
            }
            else if (cls == Timestamp.class) {
                packInt(ClientDataType.TIMESTAMP);
                packTimestamp(((java.util.Date)arg).toInstant());
            }
            else if (cls == BigDecimal.class) {
                packInt(ClientDataType.DECIMAL);
                packDecimal(((BigDecimal)arg));
            }
            else if (cls == BigInteger.class) {
                packInt(ClientDataType.BIGINTEGER);
                packBigInteger(((BigInteger)arg));
            }
            else
                throw new UnsupportedOperationException("Custom objects are not supported");
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (closed)
            return;

        closed = true;

        if (buf.refCnt() > 0)
            buf.release();
    }
}
