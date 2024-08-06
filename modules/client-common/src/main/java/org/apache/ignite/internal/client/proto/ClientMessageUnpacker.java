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

import static org.msgpack.core.MessagePack.Code;

import io.netty.buffer.ByteBuf;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleMarshalling;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.msgpack.core.ExtensionTypeHeader;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageFormatException;
import org.msgpack.core.MessageNeverUsedFormatException;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageSizeException;
import org.msgpack.core.MessageTypeException;

/**
 * ByteBuf-based MsgPack implementation. Replaces {@link org.msgpack.core.MessageUnpacker} to avoid extra buffers and indirection.
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessageUnpacker implements AutoCloseable {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Ref count. */
    private int refCnt = 1;

    /**
     * Constructor.
     *
     * @param buf Input.
     */
    public ClientMessageUnpacker(ByteBuf buf) {
        assert buf != null;

        this.buf = buf;
    }

    /**
     * Creates an overflow exception.
     *
     * @param u32 int value.
     * @return Excetion.
     */
    private static MessageSizeException overflowU32Size(int u32) {
        long lv = (long) (u32 & 0x7fffffff) + 0x80000000L;
        return new MessageSizeException(lv);
    }

    /**
     * Create an exception for the case when an unexpected byte value is read.
     *
     * @param expected Expected format.
     * @param b        Actual format.
     * @return Exception to throw.
     */
    private static MessagePackException unexpected(String expected, byte b) {
        MessageFormat format = MessageFormat.valueOf(b);

        if (format == MessageFormat.NEVER_USED) {
            return new MessageNeverUsedFormatException(String.format("Expected %s, but encountered 0xC1 \"NEVER_USED\" byte", expected));
        } else {
            String name = format.getValueType().name();
            String typeName = name.charAt(0) + name.substring(1).toLowerCase();
            return new MessageTypeException(String.format("Expected %s, but got %s (%02x)", expected, typeName, b));
        }
    }

    /**
     * Reads an int.
     *
     * @return the int value.
     * @throws MessageTypeException when value is not MessagePack Integer type.
     */
    public int unpackInt() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
                return buf.readUnsignedByte();

            case Code.INT8:
                return buf.readByte();

            case Code.UINT16:
                return buf.readUnsignedShort();

            case Code.INT16:
                return buf.readShort();

            case Code.UINT32:
            case Code.INT32:
                return buf.readInt();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads an int.
     *
     * @param defaultValue Default value to return when type is not from int family.
     * @return the int value.
     * @throws MessageTypeException when value is not MessagePack Integer type.
     */
    public int tryUnpackInt(int defaultValue) {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
                return buf.readUnsignedByte();

            case Code.INT8:
                return buf.readByte();

            case Code.UINT16:
                return buf.readUnsignedShort();

            case Code.INT16:
                return buf.readShort();

            case Code.UINT32:
            case Code.INT32:
                return buf.readInt();

            default:
                buf.readerIndex(buf.readerIndex() - 1);
                return defaultValue;
        }
    }

    /**
     * Reads a string.
     *
     * @return String value.
     */
    public String unpackString() {
        assert refCnt > 0 : "Unpacker is closed";

        int len = unpackRawStringHeader();
        int pos = buf.readerIndex();

        String res = buf.toString(pos, len, StandardCharsets.UTF_8);

        buf.readerIndex(pos + len);

        return res;
    }

    /**
     * Reads a Nil byte.
     *
     * @throws MessageTypeException when value is not MessagePack Nil type
     */
    public void unpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (code == Code.NIL) {
            return;
        }

        throw unexpected("Nil", code);
    }

    /**
     * Reads a boolean value.
     *
     * @return Boolean.
     */
    public boolean unpackBoolean() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FALSE:
                return false;

            case Code.TRUE:
                return true;

            default:
                throw unexpected("boolean", code);
        }
    }

    /**
     * Reads a byte.
     *
     * @return Byte.
     */
    public byte unpackByte() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
            case Code.INT8:
                return buf.readByte();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a short value.
     *
     * @return Short.
     */
    public short unpackShort() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
                return buf.readUnsignedByte();

            case Code.INT8:
                return buf.readByte();

            case Code.UINT16:
            case Code.INT16:
                return buf.readShort();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a long value.
     *
     * @return Long.
     */
    public long unpackLong() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
                return buf.readUnsignedByte();

            case Code.INT8:
                return buf.readByte();

            case Code.UINT16:
                return buf.readUnsignedShort();

            case Code.INT16:
                return buf.readShort();

            case Code.UINT32:
                return buf.readUnsignedInt();

            case Code.INT32:
                return buf.readInt();

            case Code.UINT64:
            case Code.INT64:
                return buf.readLong();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a float value.
     *
     * @return Float.
     */
    public float unpackFloat() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FLOAT32:
                return buf.readFloat();

            case Code.FLOAT64:
                return (float) buf.readDouble();

            default:
                throw unexpected("Float", code);
        }
    }

    /**
     * Reads a double value.
     *
     * @return Double.
     */
    public double unpackDouble() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FLOAT32:
                return buf.readFloat();

            case Code.FLOAT64:
                return buf.readDouble();

            default:
                throw unexpected("Float", code);
        }
    }

    /**
     * Reads an extension type header.
     *
     * @return Extension type header.
     */
    public ExtensionTypeHeader unpackExtensionTypeHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FIXEXT1: {
                return new ExtensionTypeHeader(buf.readByte(), 1);
            }

            case Code.FIXEXT2: {
                return new ExtensionTypeHeader(buf.readByte(), 2);
            }

            case Code.FIXEXT4: {
                return new ExtensionTypeHeader(buf.readByte(), 4);
            }

            case Code.FIXEXT8: {
                return new ExtensionTypeHeader(buf.readByte(), 8);
            }

            case Code.FIXEXT16: {
                return new ExtensionTypeHeader(buf.readByte(), 16);
            }

            case Code.EXT8: {
                int length = readLength8();
                byte type = buf.readByte();

                return new ExtensionTypeHeader(type, length);
            }

            case Code.EXT16: {
                int length = readLength16();
                byte type = buf.readByte();

                return new ExtensionTypeHeader(type, length);
            }

            case Code.EXT32: {
                int length = readLength32();
                byte type = buf.readByte();

                return new ExtensionTypeHeader(type, length);
            }

            default:
                throw unexpected("Ext", code);
        }
    }

    /**
     * Reads a binary header.
     *
     * @return Binary payload size.
     */
    public int unpackBinaryHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixedRaw(code)) { // FixRaw
            return code & 0x1f;
        }

        switch (code) {
            case Code.BIN8:
                return readLength8();

            case Code.BIN16:
                return readLength16();

            case Code.BIN32:
                return readLength32();

            default:
                throw unexpected("Binary", code);
        }
    }

    /**
     * Tries to read a nil value.
     *
     * @return True when there was a nil value, false otherwise.
     */
    public boolean tryUnpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        int idx = buf.readerIndex();
        byte code = buf.getByte(idx);

        if (code == Code.NIL) {
            buf.readerIndex(idx + 1);
            return true;
        }

        return false;
    }

    /**
     * Reads a payload.
     *
     * @param length Payload size.
     * @return Payload bytes.
     */
    public byte[] readPayload(int length) {
        assert refCnt > 0 : "Unpacker is closed";

        byte[] res = new byte[length];
        buf.readBytes(res);

        return res;
    }

    /**
     * Reads a payload into the specified buffer.
     *
     * @param target Target buffer.
     */
    public void readPayload(ByteBuffer target) {
        assert refCnt > 0 : "Unpacker is closed";

        buf.readBytes(target);
    }

    /**
     * Reads a binary value.
     *
     * @return Payload bytes.
     */
    public byte[] readBinary() {
        assert refCnt > 0 : "Unpacker is closed";

        var length = unpackBinaryHeader();
        return readPayload(length);
    }

    /**
     * Reads a binary value.
     * NOTE: Exposes internal pooled buffer to avoid copying. The buffer is not valid after current instance is closed.
     *
     * @return Payload bytes.
     */
    public ByteBuffer readBinaryUnsafe() {
        assert refCnt > 0 : "Unpacker is closed";

        var length = unpackBinaryHeader();
        var idx = buf.readerIndex();

        // TODO IGNITE-17821 Thin 3.0 Perf: Implement BinaryTupleReader and Builder over ByteBuf.
        // Note: this may or may not avoid the actual copy.
        ByteBuffer byteBuffer = buf.internalNioBuffer(idx, length).slice();
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        buf.readerIndex(idx + length);

        return byteBuffer;
    }

    /**
     * Skips values.
     *
     * @param count Number of values to skip.
     */
    public void skipValues(int count) {
        assert refCnt > 0 : "Unpacker is closed";

        while (count > 0) {
            byte code = buf.readByte();
            MessageFormat f = MessageFormat.valueOf(code);

            switch (f) {
                case POSFIXINT:
                case NEGFIXINT:
                case BOOLEAN:
                case NIL:
                    break;

                case FIXMAP: {
                    int mapLen = code & 0x0f;
                    count += mapLen * 2;
                    break;
                }

                case FIXARRAY: {
                    int arrayLen = code & 0x0f;
                    count += arrayLen;
                    break;
                }

                case FIXSTR: {
                    int strLen = code & 0x1f;
                    skipBytes(strLen);
                    break;
                }

                case INT8:
                case UINT8:
                    skipBytes(1);
                    break;

                case INT16:
                case UINT16:
                    skipBytes(2);
                    break;

                case INT32:
                case UINT32:
                case FLOAT32:
                    skipBytes(4);
                    break;

                case INT64:
                case UINT64:
                case FLOAT64:
                    skipBytes(8);
                    break;

                case BIN8:
                case STR8:
                    skipBytes(readLength8());
                    break;

                case BIN16:
                case STR16:
                    skipBytes(readLength16());
                    break;

                case BIN32:
                case STR32:
                    skipBytes(readLength32());
                    break;

                case FIXEXT1:
                    skipBytes(2);
                    break;

                case FIXEXT2:
                    skipBytes(3);
                    break;

                case FIXEXT4:
                    skipBytes(5);
                    break;

                case FIXEXT8:
                    skipBytes(9);
                    break;

                case FIXEXT16:
                    skipBytes(17);
                    break;

                case EXT8:
                    skipBytes(readLength8() + 1);
                    break;

                case EXT16:
                    skipBytes(readLength16() + 1);
                    break;

                case EXT32:
                    skipBytes(readLength32() + 1);
                    break;

                case ARRAY16:
                    count += readLength16();
                    break;

                case ARRAY32:
                    count += readLength32();
                    break;

                case MAP16:
                    count += readLength16() * 2;
                    break;

                case MAP32:
                    count += readLength32() * 2;
                    break;

                default:
                    throw new MessageFormatException("Unexpected format code: " + code);
            }

            count--;
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

        if (type != ClientMsgPackType.UUID) {
            throw new MessageTypeException("Expected UUID extension (3), but got " + type);
        }

        if (len != 16) {
            throw new MessageSizeException("Expected 16 bytes for UUID extension, but got " + len, len);
        }

        return new UUID(buf.readLongLE(), buf.readLongLE());
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

        if (type != ClientMsgPackType.BITMASK) {
            throw new MessageTypeException("Expected BITSET extension (7), but got " + type);
        }

        var bytes = readPayload(len);

        return BitSet.valueOf(bytes);
    }

    /**
     * Reads a nullable bit set.
     *
     * @return Bit set or null.
     * @throws MessageTypeException when type is not BitSet.
     */
    public @Nullable BitSet unpackBitSetNullable() {
        return tryUnpackNil() ? null : unpackBitSet();
    }

    /**
     * Reads an integer array.
     *
     * @return Integer array.
     */
    public int[] unpackIntArray() {
        assert refCnt > 0 : "Unpacker is closed";

        int size = unpackInt();

        if (size == 0) {
            return ArrayUtils.INT_EMPTY_ARRAY;
        }

        int[] res = new int[size];

        for (int i = 0; i < size; i++) {
            res[i] = unpackInt();
        }

        return res;
    }

    /**
     * Reads array of longs.
     *
     * @return Array of longs.
     */
    public long[] unpackLongArray() {
        assert refCnt > 0 : "Unpacker is closed";

        int size = unpackInt();

        if (size == 0) {
            return ArrayUtils.LONG_EMPTY_ARRAY;
        }

        long[] res = new long[size];

        for (int i = 0; i < size; i++) {
            res[i] = unpackInt();
        }

        return res;
    }

    /**
     * Unpacks batch of arguments from binary tuples.
     *
     * @return BatchedArguments object with the unpacked arguments.
     */
    @SuppressWarnings("unused")
    public BatchedArguments unpackBatchedArgumentsFromBinaryTupleArray() {
        assert refCnt > 0 : "Unpacker is closed";

        if (tryUnpackNil()) {
            return null;
        }

        int rowLen = unpackInt();
        int rows = unpackInt();
        unpackBoolean(); // unused now, but we will need it in case of arguments load by pages.

        BatchedArguments args = BatchedArguments.create();

        for (int i = 0; i < rows; i++) {
            args.add(unpackObjectArrayFromBinaryTuple(rowLen));
        }

        return args;
    }

    /**
     * Unpacks object array.
     *
     * @param size Array size.
     *
     * @return Object array.
     */
    public Object[] unpackObjectArrayFromBinaryTuple(int size) {
        assert refCnt > 0 : "Unpacker is closed";

        if (tryUnpackNil()) {
            return null;
        }

        Object[] args = new Object[size];
        var reader = new BinaryTupleReader(size * 3, readBinaryUnsafe());

        for (int i = 0; i < size; i++) {
            args[i] = ClientBinaryTupleUtils.readObject(reader, i * 3);
        }

        return args;
    }

    /**
     * Unpacks object array.
     *
     * @return Object array.
     */
    public Object[] unpackObjectArrayFromBinaryTuple() {
        assert refCnt > 0 : "Unpacker is closed";

        if (tryUnpackNil()) {
            return null;
        }

        int size = unpackInt();

        if (size == 0) {
            return ArrayUtils.OBJECT_EMPTY_ARRAY;
        }

        Object[] args = new Object[size];
        var reader = new BinaryTupleReader(size * 3, readBinaryUnsafe());

        for (int i = 0; i < size; i++) {
            args[i] = ClientBinaryTupleUtils.readObject(reader, i * 3);
        }

        return args;
    }

    /**
     * Unpacks object.
     *
     * @return Object.
     */
    public Object unpackObjectFromBinaryTuple() {
        assert refCnt > 0 : "Unpacker is closed";

        if (tryUnpackNil()) {
            return null;
        }

        var reader = new BinaryTupleReader(3, readBinaryUnsafe());
        return ClientBinaryTupleUtils.readObject(reader, 0);
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
    @Override
    public void close() {
        if (refCnt == 0) {
            return;
        }

        refCnt--;

        if (buf.refCnt() > 0) {
            buf.release();
        }
    }

    /**
     * Unpacks string header.
     *
     * @return String length.
     */
    public int unpackRawStringHeader() {
        byte code = buf.readByte();

        if (Code.isFixedRaw(code)) {
            return code & 0x1f;
        }

        switch (code) {
            case Code.STR8:
                return readLength8();

            case Code.STR16:
                return readLength16();

            case Code.STR32:
                return readLength32();

            default:
                throw unexpected("String", code);
        }
    }

    /**
     * Reads a nullable {@link Instant}.
     *
     * @return {@link Instant} value or {@code null}.
     */
    public @Nullable Instant unpackInstantNullable() {
        if (tryUnpackNil()) {
            return null;
        }
        return unpackInstant();
    }

    /**
     * Reads a nullable byte.
     *
     * @return Byte value or {@code null}.
     */
    public @Nullable Byte unpackByteNullable() {
        if (tryUnpackNil()) {
            return null;
        }
        return unpackByte();
    }

    /**
     * Reads a nullable string.
     *
     * @return String value or {@code null}.
     */
    public @Nullable String unpackStringNullable() {
        if (tryUnpackNil()) {
            return null;
        }
        return unpackString();
    }

    /**
     * Reads an {@link Instant}.
     *
     * @return {@link Instant} value.
     */
    public Instant unpackInstant() {
        long seconds = unpackLong();
        int nanos = unpackInt();
        return Instant.ofEpochSecond(seconds, nanos);
    }

    /**
     * Unpacks deployment units.
     *
     * @return Deployment units.
     */
    public List<DeploymentUnit> unpackDeploymentUnits() {
        int size = tryUnpackNil() ? 0 : unpackInt();
        List<DeploymentUnit> res = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            res.add(new DeploymentUnit(unpackString(), unpackString()));
        }

        return res;
    }

    private int readLength8() {
        return buf.readUnsignedByte();
    }

    private int readLength16() {
        return buf.readUnsignedShort();
    }

    private int readLength32() {
        int u32 = buf.readInt();

        if (u32 < 0) {
            throw overflowU32Size(u32);
        }

        return u32;
    }

    private void skipBytes(int bytes) {
        buf.readerIndex(buf.readerIndex() + bytes);
    }

    public Object unpackJobArgument() {
        int typeId = unpackInt();
        if (typeId == -1) {
            unpackNil();
            return null;
        }

        var type = JobArgumentTypeId.Type.fromId(typeId);
        switch (type) {
            case NATIVE:
                ColumnType columnType = ColumnType.getById(typeId);
                return unpackNativeType(columnType);
            case MARSHALLED_TUPLE:
                return TupleMarshalling.unmarshal(readBinary());
            default:
                throw new IllegalArgumentException("Unsupported type id: " + typeId);
        }
    }

    private Object unpackNativeType(ColumnType type) {
        switch (type) {
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
            case DECIMAL:
                int scale = unpackInt();
                byte[] unscaledBytes = readBinary();
                BigInteger unscaled = new BigInteger(unscaledBytes);
                return new BigDecimal(unscaled, scale);
            case UUID:
                return unpackUuid();
            case STRING:
                return unpackString();
            case BYTE_ARRAY:
                return readBinary();
            case DATE:
                return null;
            case TIME:
                return null;
            case DATETIME:
                return null;
            case TIMESTAMP:
                return null;
            case DURATION:
                return null;
            case PERIOD:
                return null;
            default:
                throw new IllegalArgumentException();

        }
    }

    public Object unpackJobResult() {
        int typeId = unpackInt();

        var type = JobArgumentTypeId.Type.fromId(typeId);
        switch (type) {
            case NATIVE:
                ColumnType columnType = ColumnType.getById(typeId);
                return unpackNativeType(columnType);
            case MARSHALLED_TUPLE:
                return TupleMarshalling.unmarshal(readBinary());
            default:
                throw new IllegalArgumentException("Unsupported type id: " + typeId);
        }
    }
}
