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
import static org.msgpack.core.MessagePack.Code;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.jetbrains.annotations.Nullable;

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
     * Metadata.
     */
    private @Nullable Object meta;

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
     * Writes an int value.
     *
     * @param i the value to be written.
     */
    public void packIntNullable(Integer i) {
        if (i == null) {
            packNil();
        } else {
            packInt(i);
        }
    }

    /**
     * Reserve space for long value.
     *
     * @return Index of reserved space.
     */
    public int reserveLong() {
        buf.writeByte(Code.INT64);
        var index = buf.writerIndex();

        buf.writeLong(0);
        return index;
    }

    /**
     * Set long value at reserved index (see {@link #reserveLong()}).
     *
     * @param index Index.
     * @param v Value.
     */
    public void setLong(int index, long v) {
        buf.setLong(index, v);
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
     * Writes a long value.
     *
     * @param v the value to be written.
     */
    public void packLongNullable(@Nullable Long v) {
        if (v == null) {
            packNil();
        } else {
            packLong(v);
        }
    }

    /**
     * Writes a byte value.
     *
     * @param v the value to be written.
     */
    public void packByteNullable(@Nullable Byte v) {
        if (v == null) {
            packNil();
        } else {
            packByte(v);
        }
    }

    /**
     * Writes a string value.
     *
     * @param v the value to be written.
     */
    public void packStringNullable(@Nullable String v) {
        if (v == null) {
            packNil();
        } else {
            packString(v);
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
    public void packString(@Nullable String s) {
        assert !closed : "Packer is closed";

        if (s == null) {
            packNil();
            return;
        }

        // Header is a varint.
        // Use pessimistic utf8MaxBytes to reserve bytes for the header.
        // This may cause an extra byte to be used for the header,
        // but this is cheaper than calculating correct utf8 byte size, which involves full string scan.
        int maxBytes = ByteBufUtil.utf8MaxBytes(s);
        int headerSize = getStringHeaderSize(maxBytes);
        int headerPos = buf.writerIndex();

        int index = headerPos + headerSize;
        if (index > buf.capacity()) {
            buf.capacity(buf.capacity() * 2);
        }
        buf.writerIndex(index);

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

        buf.writeLongLE(val.getMostSignificantBits());
        buf.writeLongLE(val.getLeastSignificantBits());
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
     * Writes a bit set.
     *
     * @param val Bit set value.
     */
    public void packBitSetNullable(@Nullable BitSet val) {
        if (val == null) {
            packNil();
        } else {
            packBitSet(val);
        }
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

        packInt(arr.length);

        for (int i : arr) {
            packInt(i);
        }
    }

    /**
     * Writes an long array.
     *
     * @param arr Long array value.
     */
    public void packLongArray(long[] arr) {
        assert !closed : "Packer is closed";

        if (arr == null) {
            packNil();

            return;
        }

        packInt(arr.length);

        for (long i : arr) {
            packLong(i);
        }
    }

    /**
     * Packs an array of objects in BinaryTuple format.
     *
     * @param input Object array.
     */
    public void packObjectArrayAsBinaryTuple(Object @Nullable [] input) {
        assert !closed : "Packer is closed";

        if (input == null) {
            packNil();

            return;
        }

        packInt(input.length);

        if (input.length == 0) {
            return;
        }

        // Builder with inline schema.
        // Every element in vals is represented by 3 tuple elements: type, scale, value.
        var builder = new BinaryTupleBuilder(3);

        ClientBinaryTupleUtils.appendObject(builder, input);

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
        var builder = new BinaryTupleBuilder(3, 3);
        ClientBinaryTupleUtils.appendObject(builder, val);

        packBinaryTuple(builder);
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
     * @param binaryTupleParser Binary tuple parser.
     */
    public void packBinaryTuple(BinaryTupleParser binaryTupleParser) {
        ByteBuffer buf = binaryTupleParser.byteBuffer();
        int len = buf.limit() - buf.position();

        packBinaryHeader(len);
        writePayload(buf);
    }

    /**
     * Packs binary tuple.
     *
     * @param builder Builder.
     */
    public void packBinaryTuple(BinaryTupleBuilder builder) {
        ByteBuffer buf = builder.build();
        packByteBuffer(buf);
    }

    /**
     * Pack ByteBuffer.
     *
     * @param buf ByteBuffer object.
     */
    public void packByteBuffer(ByteBuffer buf) {
        assert !closed : "Packer is closed";

        packBinaryHeader(buf.limit() - buf.position());
        writePayload(buf);
    }

    /**
     * Packs an {@link Instant}.
     *
     * @param instant Instant object.
     */
    public void packInstant(@Nullable Instant instant) {
        if (instant == null) {
            packNil();
        } else {
            packLong(instant.getEpochSecond());
            packInt(instant.getNano());
        }
    }

    /**
     * Packs deployment units.
     *
     * @param units Units.
     */
    public void packDeploymentUnits(List<DeploymentUnit> units) {
        packInt(units.size());
        for (DeploymentUnit unit : units) {
            packString(unit.name());
            packString(unit.version().render());
        }
    }

    /**
     * Gets metadata.
     *
     * @return Metadata.
     */
    public @Nullable Object meta() {
        return meta;
    }

    /**
     * Sets metadata.
     *
     * @param meta Metadata.
     */
    public void meta(@Nullable Object meta) {
        this.meta = meta;
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
