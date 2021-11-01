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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteUuid;
import org.msgpack.core.ExtensionTypeHeader;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageFormatException;
import org.msgpack.core.MessageIntegerOverflowException;
import org.msgpack.core.MessageNeverUsedFormatException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageSizeException;
import org.msgpack.core.MessageTypeException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.InputStreamBufferInput;
import org.msgpack.value.ImmutableValue;

import static org.apache.ignite.internal.client.proto.ClientDataType.BIGINTEGER;
import static org.apache.ignite.internal.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.internal.client.proto.ClientDataType.BOOLEAN;
import static org.apache.ignite.internal.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATE;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATETIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.internal.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.internal.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT16;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT32;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT64;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT8;
import static org.apache.ignite.internal.client.proto.ClientDataType.NUMBER;
import static org.apache.ignite.internal.client.proto.ClientDataType.STRING;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIMESTAMP;
import static org.msgpack.core.MessagePack.Code;

/**
 * ByteBuf-based MsgPack implementation.
 * Replaces {@link org.msgpack.core.MessageUnpacker} to avoid extra buffers and indirection.
 * <p>
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessageUnpacker extends MessageUnpacker {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Underlying input. */
    private final InputStreamBufferInput in;

    /** Ref count. */
    private int refCnt = 1;

    /**
     * Constructor.
     *
     * @param buf Input.
     */
    public ClientMessageUnpacker(ByteBuf buf) {
        // TODO: Remove intermediate classes and buffers IGNITE-15234.
        this(new InputStreamBufferInput(new ByteBufInputStream(buf)), buf);
    }

    private ClientMessageUnpacker(InputStreamBufferInput in, ByteBuf buf) {
        super(in, MessagePack.DEFAULT_UNPACKER_CONFIG);

        this.in = in;
        this.buf = buf;
    }

    /** {@inheritDoc} */
    @Override public int unpackInt() {
        assert refCnt > 0 : "Unpacker is closed";

        byte b = readByte();
        if (Code.isFixInt(b)) {
            return b;
        }
        switch (b) {
            case Code.UINT8: // unsigned int 8
                byte u8 = readByte();
                return u8 & 0xff;
            case Code.UINT16: // unsigned int 16
                short u16 = readShort();
                return u16 & 0xffff;
            case Code.UINT32: // unsigned int 32
                int u32 = readInt();
                if (u32 < 0) {
                    throw overflowU32(u32);
                }
                return u32;
            case Code.UINT64: // unsigned int 64
                long u64 = readLong();
                if (u64 < 0L || u64 > (long) Integer.MAX_VALUE) {
                    throw overflowU64(u64);
                }
                return (int) u64;
            case Code.INT8: // signed int 8
                byte i8 = readByte();
                return i8;
            case Code.INT16: // signed int 16
                short i16 = readShort();
                return i16;
            case Code.INT32: // signed int 32
                int i32 = readInt();
                return i32;
            case Code.INT64: // signed int 64
                long i64 = readLong();
                if (i64 < (long) Integer.MIN_VALUE || i64 > (long) Integer.MAX_VALUE) {
                    throw overflowI64(i64);
                }
                return (int) i64;
        }
        throw unexpected("Integer", b);    }

    /** {@inheritDoc} */
    @Override public String unpackString() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void unpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            super.unpackNil();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean unpackBoolean() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackBoolean();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte unpackByte() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackByte();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short unpackShort() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackShort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long unpackLong() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackLong();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public BigInteger unpackBigInteger() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackBigInteger();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public float unpackFloat() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackFloat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public double unpackDouble() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackDouble();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackArrayHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackArrayHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackMapHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackMapHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ExtensionTypeHeader unpackExtensionTypeHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackExtensionTypeHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackBinaryHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackBinaryHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryUnpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.tryUnpackNil();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] readPayload(int length) {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.readPayload(length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessageFormat getNextFormat() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.getNextFormat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void skipValue(int count) {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            super.skipValue(count);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void skipValue() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            super.skipValue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.hasNext();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ImmutableValue unpackValue() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackValue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads an UUID.
     *
     * @return UUID value.
     * @throws MessageTypeException when type is not UUID.
     * @throws MessageSizeException when size is not correct.
     */
    public UUID unpackUuid() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.UUID)
            throw new MessageTypeException("Expected UUID extension (3), but got " + type);

        if (len != 16)
            throw new MessageSizeException("Expected 16 bytes for UUID extension, but got " + len, len);

        var bytes = readPayload(16);

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        return new UUID(bb.getLong(), bb.getLong());
    }

    /**
     * Reads an {@link IgniteUuid}.
     *
     * @return {@link IgniteUuid} value.
     * @throws MessageTypeException when type is not {@link IgniteUuid}.
     * @throws MessageSizeException when size is not correct.
     */
    public IgniteUuid unpackIgniteUuid() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.IGNITE_UUID)
            throw new MessageTypeException("Expected Ignite UUID extension (1), but got " + type);

        if (len != 24)
            throw new MessageSizeException("Expected 24 bytes for UUID extension, but got " + len, len);

        var bytes = readPayload(24);

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        return new IgniteUuid(new UUID(bb.getLong(), bb.getLong()), bb.getLong());
    }

    /**
     * Reads a decimal.
     *
     * @return Decimal value.
     * @throws MessageTypeException when type is not Decimal.
     */
    public BigDecimal unpackDecimal() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.DECIMAL)
            throw new MessageTypeException("Expected DECIMAL extension (2), but got " + type);

        var bytes = readPayload(len);

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        int scale = bb.getInt();

        return new BigDecimal(new BigInteger(bytes, bb.position(), bb.remaining()), scale);
    }

    /**
     * Reads a bit set.
     *
     * @return Bit set.
     * @throws MessageTypeException when type is not BitSet.
     */
    public BitSet unpackBitSet() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.BITMASK)
            throw new MessageTypeException("Expected BITSET extension (7), but got " + type);

        var bytes = readPayload(len);

        return BitSet.valueOf(bytes);
    }

    /**
     * Reads a number.
     *
     * @return BigInteger value.
     * @throws MessageTypeException when type is not BigInteger.
     */
    public BigInteger unpackNumber() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.NUMBER)
            throw new MessageTypeException("Expected NUMBER extension (1), but got " + type);

        var bytes = readPayload(len);

        return new BigInteger(bytes);
    }

    /**
     * Reads an integer array.
     *
     * @return Integer array.
     */
    public int[] unpackIntArray() {
        assert refCnt > 0 : "Unpacker is closed";

        int size = unpackArrayHeader();

        if (size == 0)
            return ArrayUtils.INT_EMPTY_ARRAY;

        int[] res = new int[size];

        for (int i = 0; i < size; i++)
            res[i] = unpackInt();

        return res;
    }

    /**
     * Reads a date.
     *
     * @return Date value.
     * @throws MessageTypeException when type is not DATE.
     * @throws MessageSizeException when size is not correct.
     */
    public LocalDate unpackDate() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.DATE)
            throw new MessageTypeException("Expected DATE extension (4), but got " + type);

        if (len != 6)
            throw new MessageSizeException("Expected 6 bytes for DATE extension, but got " + len, len);

        var data = ByteBuffer.wrap(readPayload(len));

        return LocalDate.of(data.getInt(), data.get(), data.get());
    }

    /**
     * Reads a time.
     *
     * @return Time value.
     * @throws MessageTypeException when type is not TIME.
     * @throws MessageSizeException when size is not correct.
     */
    public LocalTime unpackTime() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.TIME)
            throw new MessageTypeException("Expected TIME extension (5), but got " + type);

        if (len != 7)
            throw new MessageSizeException("Expected 7 bytes for TIME extension, but got " + len, len);

        var data = ByteBuffer.wrap(readPayload(len));

        return LocalTime.of(data.get(), data.get(), data.get(), data.getInt());
    }

    /**
     * Reads a datetime.
     *
     * @return Datetime value.
     * @throws MessageTypeException when type is not DATETIME.
     * @throws MessageSizeException when size is not correct.
     */
    public LocalDateTime unpackDateTime() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.DATETIME)
            throw new MessageTypeException("Expected DATETIME extension (6), but got " + type);

        if (len != 13)
            throw new MessageSizeException("Expected 13 bytes for DATETIME extension, but got " + len, len);

        var data = ByteBuffer.wrap(readPayload(len));

        return LocalDateTime.of(
            LocalDate.of(data.getInt(), data.get(), data.get()),
            LocalTime.of(data.get(), data.get(), data.get(), data.getInt())
        );
    }

    /**
     * Reads a timestamp.
     *
     * @return Timestamp value.
     * @throws MessageTypeException when type is not TIMESTAMP.
     * @throws MessageSizeException when size is not correct.
     */
    public Instant unpackTimestamp() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.TIMESTAMP)
            throw new MessageTypeException("Expected TIMESTAMP extension (6), but got " + type);

        if (len != 12)
            throw new MessageSizeException("Expected 12 bytes for TIMESTAMP extension, but got " + len, len);

        var data = ByteBuffer.wrap(readPayload(len));

        return Instant.ofEpochSecond(data.getLong(), data.getInt());
    }

    /**
     * Unpacks an object based on the specified type.
     *
     * @param dataType Data type code.
     *
     * @return Unpacked object.
     * @throws IgniteException when data type is not valid.
     */
    public Object unpackObject(int dataType) {
        if (tryUnpackNil())
            return null;

        switch (dataType) {
            case BOOLEAN:
                return unpackBoolean();

            case INT8:
                return unpackByte();

            case INT16:
                return unpackShort();

            case INT32:
                return unpackInt();

            case INT64:
                return unpackLong();

            case FLOAT:
                return unpackFloat();

            case DOUBLE:
                return unpackDouble();

            case ClientDataType.UUID:
                return unpackUuid();

            case STRING:
                return unpackString();

            case BYTES: {
                var cnt = unpackBinaryHeader();

                return readPayload(cnt);
            }

            case DECIMAL:
                return unpackDecimal();

            case BIGINTEGER:
                return unpackBigInteger();

            case BITMASK:
                return unpackBitSet();

            case NUMBER:
                return unpackNumber();

            case DATE:
                return unpackDate();

            case TIME:
                return unpackTime();

            case DATETIME:
                return unpackDateTime();

            case TIMESTAMP:
                return unpackTimestamp();
        }

        throw new IgniteException("Unknown client data type: " + dataType);
    }

    /**
     * Packs an object.
     *
     * @return Object array.
     * @throws IllegalStateException in case of unexpected value type.
     */
    public Object[] unpackObjectArray() {
        assert refCnt > 0 : "Unpacker is closed";

        if (tryUnpackNil())
            return null;

        int size = unpackArrayHeader();

        if (size == 0)
            return ArrayUtils.OBJECT_EMPTY_ARRAY;

        Object[] args = new Object[size];

        for (int i = 0; i < size; i++) {
            if (tryUnpackNil())
                continue;

            args[i] = unpackObject(unpackInt());
        }
        return args;
    }

    /**
     * Creates a copy of this unpacker and the underlying buffer.
     *
     * @return Copied unpacker.
     * @throws UncheckedIOException When buffer operation fails.
     */
    public ClientMessageUnpacker copy() {
        try {
            in.reset(new ByteBufInputStream(buf.copy()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return this;
    }

    /**
     * Increases the reference count by {@code 1}.
     *
     * @return This instance.
     */
    public ClientMessageUnpacker retain() {
        refCnt++;

        buf.retain();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (refCnt == 0)
            return;

        refCnt--;

        if (buf.refCnt() > 0)
            buf.release();
    }

    private static MessageIntegerOverflowException overflowU8(byte u8)
    {
        BigInteger bi = BigInteger.valueOf((long) (u8 & 0xff));
        return new MessageIntegerOverflowException(bi);
    }

    private static MessageIntegerOverflowException overflowU16(short u16)
    {
        BigInteger bi = BigInteger.valueOf((long) (u16 & 0xffff));
        return new MessageIntegerOverflowException(bi);
    }

    private static MessageIntegerOverflowException overflowU32(int u32)
    {
        BigInteger bi = BigInteger.valueOf((long) (u32 & 0x7fffffff) + 0x80000000L);
        return new MessageIntegerOverflowException(bi);
    }

    private static MessageIntegerOverflowException overflowU64(long u64)
    {
        BigInteger bi = BigInteger.valueOf(u64 + Long.MAX_VALUE + 1L).setBit(63);
        return new MessageIntegerOverflowException(bi);
    }

    private static MessageIntegerOverflowException overflowI16(short i16)
    {
        BigInteger bi = BigInteger.valueOf((long) i16);
        return new MessageIntegerOverflowException(bi);
    }

    private static MessageIntegerOverflowException overflowI32(int i32)
    {
        BigInteger bi = BigInteger.valueOf((long) i32);
        return new MessageIntegerOverflowException(bi);
    }

    private static MessageIntegerOverflowException overflowI64(long i64)
    {
        BigInteger bi = BigInteger.valueOf(i64);
        return new MessageIntegerOverflowException(bi);
    }

    private static MessageSizeException overflowU32Size(int u32)
    {
        long lv = (long) (u32 & 0x7fffffff) + 0x80000000L;
        return new MessageSizeException(lv);
    }

    /**
     * Create an exception for the case when an unexpected byte value is read
     *
     * @param expected
     * @param b
     * @return
     * @throws MessageFormatException
     */
    private static MessagePackException unexpected(String expected, byte b)
    {
        MessageFormat format = MessageFormat.valueOf(b);
        if (format == MessageFormat.NEVER_USED) {
            return new MessageNeverUsedFormatException(String.format("Expected %s, but encountered 0xC1 \"NEVER_USED\" byte", expected));
        }
        else {
            String name = format.getValueType().name();
            String typeName = name.substring(0, 1) + name.substring(1).toLowerCase();
            return new MessageTypeException(String.format("Expected %s, but got %s (%02x)", expected, typeName, b));
        }
    }

    private byte readByte() {
        return buf.readByte();
    }

    private short readShort() {
        return buf.readShort();
    }

    private int readInt() {
        return buf.readInt();
    }

    private long readLong() {
        return buf.readLong();
    }
}
