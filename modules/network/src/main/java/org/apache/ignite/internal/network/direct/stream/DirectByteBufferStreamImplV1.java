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

package org.apache.ignite.internal.network.direct.stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.network.NetworkMessage.NULL_GROUP_TYPE;
import static org.apache.ignite.internal.util.ArrayUtils.BOOLEAN_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.CHAR_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.DOUBLE_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.FLOAT_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.INT_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.LONG_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.SHORT_ARRAY;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.CHAR_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.DOUBLE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.FLOAT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.INT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.IS_BIG_ENDIAN;
import static org.apache.ignite.internal.util.GridUnsafe.LONG_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.SHORT_ARR_OFF;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageDeserializer;
import org.apache.ignite.internal.network.serialization.MessageReader;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.network.serialization.MessageWriter;
import org.apache.ignite.internal.util.ArrayFactory;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.jetbrains.annotations.Nullable;

/**
 * {@link DirectByteBufferStream} implementation.
 */
public class DirectByteBufferStreamImplV1 implements DirectByteBufferStream {
    /** Poison object. */
    private static final Object NULL = new Object();

    /** Flag that indicates that byte buffer is not null. */
    protected static final byte BYTE_BUFFER_NOT_NULL_FLAG = 1;

    /** Flag that indicates that byte buffer has Big Endinan order. */
    protected static final byte BYTE_BUFFER_BIG_ENDIAN_FLAG = 2;

    /** {@code Short.SIZE / 7} rounded up. */
    private static final int MAX_VAR_SHORT_BYTES = 3;

    /** {@code Integer.SIZE / 7} rounded up. */
    private static final int MAX_VAR_INT_BYTES = 5;

    /** {@code Long.SIZE / 7} rounded up. */
    private static final int MAX_VAR_LONG_BYTES = 10;

    /**
     * Offset to the {@code position} field of {@link Buffer}.
     *
     * @see #setPosition(int)
     */
    private static final long POSITION_OFF;

    static {
        try {
            Field position = Buffer.class.getDeclaredField("position");

            AccessController.doPrivileged((PrivilegedAction<?>) () -> {
                position.setAccessible(true);
                return null;
            });

            POSITION_OFF = GridUnsafe.objectFieldOffset(position);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Message serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    protected ByteBuffer buf;

    /**
     * Buffer limit is cached in a field because of frequent use of {@link #remainingInternal()}.
     * {@link Buffer#remaining()} uses conditional branching, which is expensive in our case, so we re-implemented it manually.
     */
    private int bufLimit;

    protected byte @Nullable [] heapArr;

    protected long baseOff;

    private int arrOff = -1;

    private Object tmpArr;

    private int tmpArrOff;

    /** Number of bytes of the boundary value, read from previous message. */
    private int valReadBytes;

    private int tmpArrBytes;

    /**
     * When {@code true}, this flag indicates that {@link #msgGroupType} contains a valid part of the currently read message header. {@code
     * false} means that {@link #msgGroupType} might contain some leftover data from previous reads and can be discarded.
     */
    private boolean msgGroupTypeRead;

    /**
     * Group type of the message that is currently being received.
     *
     * <p>This field saves the partial message header, because it is not received in one piece, but rather in two: message group type and
     * message type.
     */
    private short msgGroupType;

    /**
     * Flag needed for reading boxed primitives.
     *
     * <p>Boxed primitives are encoded as a boolean flag ({@code false} meaning that the boxed value is {@code null}), followed by the
     * unboxed value (if not null). Therefore, this flag value must be cached between two unsuccessful read calls in case the received boxed
     * primitive was not {@code null}.
     */
    private boolean boxedTypeNotNull;

    @Nullable
    private MessageDeserializer<NetworkMessage> msgDeserializer;

    @Nullable
    private MessageSerializer<NetworkMessage> msgSerializer;

    private Iterator<?> mapIt;

    private Iterator<?> it;

    private int arrPos = -1;

    private Object arrCur = NULL;

    private Object mapCur = NULL;

    private Object cur = NULL;

    private boolean keyDone;

    private int readSize = -1;

    private int readItems;

    private Object[] objArr;

    private Collection<Object> col;

    private Map<Object, Object> map;

    private long prim;

    private int primShift;

    private int uuidState;

    private long uuidMost;

    private long uuidLeast;

    private long uuidLocId;

    private int byteBufferState;

    private byte byteBufferFlag;

    protected boolean lastFinished;

    /** byte-array representation of string. */
    private byte[] curStrBackingArr;

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization service.       .
     */
    public DirectByteBufferStreamImplV1(MessageSerializationRegistry serializationRegistry) {
        this.serializationRegistry = Objects.requireNonNull(serializationRegistry, "serializationRegistry");
    }

    /** {@inheritDoc} */
    @Override
    public void setBuffer(ByteBuffer buf) {
        assert buf != null;

        if (this.buf != buf) {
            this.buf = buf;
            this.bufLimit = buf.limit();

            boolean isDirect = buf.isDirect();
            heapArr = isDirect ? null : buf.array();
            baseOff = isDirect ? GridUnsafe.bufferAddress(buf) : BYTE_ARR_OFF + buf.arrayOffset();
        }
    }

    /** {@inheritDoc} */
    @Override
    public int remaining() {
        return remainingInternal();
    }

    private int remainingInternal() {
        return bufLimit - buf.position();
    }

    /** {@inheritDoc} */
    @Override
    public boolean lastFinished() {
        return lastFinished;
    }

    /** {@inheritDoc} */
    @Override
    public void writeByte(byte val) {
        lastFinished = remainingInternal() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            GridUnsafe.putByte(heapArr, baseOff + pos, val);

            setPosition(pos + 1);
        }
    }

    @Override
    public void writeBoxedByte(@Nullable Byte val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + 1;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeByte(val);
            }
        } else {
            writeBoolean(false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeShort(short val) {
        lastFinished = remainingInternal() >= MAX_VAR_SHORT_BYTES;

        writeVarInt(Short.toUnsignedInt((short) (val + 1)));
    }

    @Override
    public void writeBoxedShort(@Nullable Short val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + MAX_VAR_SHORT_BYTES;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeShort(val);
            }
        } else {
            writeBoolean(false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeInt(int val) {
        lastFinished = remainingInternal() >= MAX_VAR_INT_BYTES;

        writeVarInt(val + 1);
    }

    @Override
    public void writeBoxedInt(@Nullable Integer val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + MAX_VAR_INT_BYTES;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeInt(val);
            }
        } else {
            writeBoolean(false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeLong(long val) {
        lastFinished = remainingInternal() >= MAX_VAR_LONG_BYTES;

        writeVarLong(val + 1);
    }

    @Override
    public void writeBoxedLong(@Nullable Long val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + MAX_VAR_LONG_BYTES;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeVarLong(val + 1);
            }
        } else {
            writeBoolean(false);
        }
    }

    private void writeVarLong(long val) {
        if (lastFinished) {
            int pos = buf.position();

            // Please check benchmarks if you're going to change this code.
            long shift;
            //noinspection NestedAssignment
            while ((shift = (val >>> 7)) != 0L) {
                byte b = (byte) (((int) val) | 0x80);

                GridUnsafe.putByte(heapArr, baseOff + pos++, b);

                val = shift;
            }

            GridUnsafe.putByte(heapArr, baseOff + pos++, (byte) val);

            setPosition(pos);
        }
    }

    private void writeVarInt(int val) {
        if (lastFinished) {
            int pos = buf.position();

            // Please check benchmarks if you're going to change this code.
            int shift;
            //noinspection NestedAssignment
            while ((shift = (val >>> 7)) != 0) {
                byte b = (byte) (val | 0x80);

                GridUnsafe.putByte(heapArr, baseOff + pos++, b);

                val = shift;
            }

            GridUnsafe.putByte(heapArr, baseOff + pos++, (byte) val);

            setPosition(pos);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloat(float val) {
        lastFinished = remainingInternal() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (IS_BIG_ENDIAN) {
                GridUnsafe.putFloatLittleEndian(heapArr, off, val);
            } else {
                GridUnsafe.putFloat(heapArr, off, val);
            }

            setPosition(pos + 4);
        }
    }

    @Override
    public void writeBoxedFloat(@Nullable Float val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + 4;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeFloat(val);
            }
        } else {
            writeBoolean(false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeDouble(double val) {
        lastFinished = remainingInternal() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (IS_BIG_ENDIAN) {
                GridUnsafe.putDoubleLittleEndian(heapArr, off, val);
            } else {
                GridUnsafe.putDouble(heapArr, off, val);
            }

            setPosition(pos + 8);
        }
    }

    @Override
    public void writeBoxedDouble(@Nullable Double val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + 8;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeDouble(val);
            }
        } else {
            writeBoolean(false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeChar(char val) {
        lastFinished = remainingInternal() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (IS_BIG_ENDIAN) {
                GridUnsafe.putCharLittleEndian(heapArr, off, val);
            } else {
                GridUnsafe.putChar(heapArr, off, val);
            }

            setPosition(pos + 2);
        }
    }

    @Override
    public void writeBoxedChar(@Nullable Character val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + 2;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeChar(val);
            }
        } else {
            writeBoolean(false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeBoolean(boolean val) {
        lastFinished = remainingInternal() >= 1;

        if (lastFinished) {
            writeBooleanUnchecked(val);
        }
    }

    private void writeBooleanUnchecked(boolean val) {
        int pos = buf.position();

        GridUnsafe.putByte(heapArr, baseOff + pos, (byte) (val ? 1 : 0));

        setPosition(pos + 1);
    }

    protected void setPosition(int pos) {
        // "buf.position(pos)" uses conditional branching, which is expensive for such a frequent operation.
        GridUnsafe.putIntField(buf, POSITION_OFF, pos);
    }

    @Override
    public void writeBoxedBoolean(@Nullable Boolean val) {
        if (val != null) {
            lastFinished = remainingInternal() >= 1 + 1;

            if (lastFinished) {
                writeBooleanUnchecked(true);

                writeBoolean(val);
            }
        } else {
            writeBoolean(false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeByteArray(byte[] val) {
        if (val != null) {
            lastFinished = writeArray(val, BYTE_ARR_OFF, val.length, val.length);
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeByteArray(byte[] val, long off, int len) {
        if (val != null) {
            lastFinished = writeArray(val, BYTE_ARR_OFF + off, len, len);
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeShortArray(short[] val) {
        if (val != null) {
            if (IS_BIG_ENDIAN) {
                lastFinished = writeArrayLittleEndian(val, SHORT_ARR_OFF, val.length, 2, 1);
            } else {
                lastFinished = writeArray(val, SHORT_ARR_OFF, val.length, val.length << 1);
            }
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeIntArray(int[] val) {
        if (val != null) {
            if (IS_BIG_ENDIAN) {
                lastFinished = writeArrayLittleEndian(val, INT_ARR_OFF, val.length, 4, 2);
            } else {
                lastFinished = writeArray(val, INT_ARR_OFF, val.length, val.length << 2);
            }
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeLongArray(long[] val) {
        if (val != null) {
            if (IS_BIG_ENDIAN) {
                lastFinished = writeArrayLittleEndian(val, LONG_ARR_OFF, val.length, 8, 3);
            } else {
                lastFinished = writeArray(val, LONG_ARR_OFF, val.length, val.length << 3);
            }
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeLongArray(long[] val, int len) {
        if (val != null) {
            if (IS_BIG_ENDIAN) {
                lastFinished = writeArrayLittleEndian(val, LONG_ARR_OFF, len, 8, 3);
            } else {
                lastFinished = writeArray(val, LONG_ARR_OFF, len, len << 3);
            }
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloatArray(float[] val) {
        if (val != null) {
            if (IS_BIG_ENDIAN) {
                lastFinished = writeArrayLittleEndian(val, FLOAT_ARR_OFF, val.length, 4, 2);
            } else {
                lastFinished = writeArray(val, FLOAT_ARR_OFF, val.length, val.length << 2);
            }
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeDoubleArray(double[] val) {
        if (val != null) {
            if (IS_BIG_ENDIAN) {
                lastFinished = writeArrayLittleEndian(val, DOUBLE_ARR_OFF, val.length, 8, 3);
            } else {
                lastFinished = writeArray(val, DOUBLE_ARR_OFF, val.length, val.length << 3);
            }
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeCharArray(char[] val) {
        if (val != null) {
            if (IS_BIG_ENDIAN) {
                lastFinished = writeArrayLittleEndian(val, CHAR_ARR_OFF, val.length, 2, 1);
            } else {
                lastFinished = writeArray(val, CHAR_ARR_OFF, val.length, val.length << 1);
            }
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeBooleanArray(boolean[] val) {
        if (val != null) {
            lastFinished = writeArray(val, GridUnsafe.BOOLEAN_ARR_OFF, val.length, val.length);
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeString(String val) {
        if (val != null) {
            if (curStrBackingArr == null) {
                curStrBackingArr = val.getBytes(UTF_8);
            }

            writeByteArray(curStrBackingArr);

            if (lastFinished) {
                curStrBackingArr = null;
            }
        } else {
            writeByteArray(null);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeBitSet(BitSet val) {
        writeLongArray(val != null ? val.toLongArray() : null);
    }

    /** {@inheritDoc} */
    @Override
    public void writeByteBuffer(ByteBuffer val) {
        switch (byteBufferState) {
            case 0:
                byte flag = 0;

                if (val != null) {
                    flag |= BYTE_BUFFER_NOT_NULL_FLAG;

                    if (val.order() == ByteOrder.BIG_ENDIAN) {
                        flag |= BYTE_BUFFER_BIG_ENDIAN_FLAG;
                    }
                }

                writeByte(flag);

                if (!lastFinished || val == null) {
                    return;
                }

                byteBufferState++;

                //noinspection fallthrough
            case 1:
                assert !val.isReadOnly();

                int position = val.position();
                int length = val.limit() - position;

                if (val.isDirect()) {
                    lastFinished = writeArray(null, GridUnsafe.bufferAddress(val) + position, length, length);
                } else {
                    lastFinished = writeArray(val.array(), BYTE_ARR_OFF + val.arrayOffset() + position, length, length);
                }

                if (!lastFinished) {
                    return;
                }

                byteBufferState = 0;
                break;

            default:
                throw new IllegalArgumentException("Unknown byteBufferState: " + byteBufferState);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeUuid(UUID val) {
        if (val == null) {
            writeBoolean(true);
        } else {
            lastFinished = remainingInternal() >= 1 + 2 * Long.BYTES;

            if (lastFinished) {
                int pos = buf.position();

                GridUnsafe.putBoolean(heapArr, baseOff + pos, false);
                GridUnsafe.putLongLittleEndian(heapArr, baseOff + pos + 1, val.getMostSignificantBits());
                GridUnsafe.putLongLittleEndian(heapArr, baseOff + pos + 9, val.getLeastSignificantBits());

                setPosition(pos + 1 + 2 * Long.BYTES);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeIgniteUuid(IgniteUuid val) {
        switch (uuidState) {
            case 0:
                writeBoolean(val == null);

                if (!lastFinished || val == null) {
                    return;
                }

                uuidState++;

                //noinspection fallthrough
            case 1:
                writeLong(val.globalId().getMostSignificantBits());

                if (!lastFinished) {
                    return;
                }

                uuidState++;

                //noinspection fallthrough
            case 2:
                writeLong(val.globalId().getLeastSignificantBits());

                if (!lastFinished) {
                    return;
                }

                uuidState++;

                //noinspection fallthrough
            case 3:
                writeLong(val.localId());

                if (!lastFinished) {
                    return;
                }

                uuidState = 0;
                break;

            default:
                throw new IllegalArgumentException("Unknown uuidState: " + uuidState);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeMessage(NetworkMessage msg, MessageWriter writer) {
        if (msg != null) {
            if (buf.hasRemaining()) {
                try {
                    writer.beforeInnerMessageWrite();

                    writer.setCurrentWriteClass(msg.getClass());

                    if (msgSerializer == null) {
                        msgSerializer = msg.serializer();
                    }

                    writer.setBuffer(buf);

                    lastFinished = msgSerializer.writeMessage(msg, writer);

                    if (lastFinished) {
                        msgSerializer = null;
                    }
                } finally {
                    writer.afterInnerMessageWrite(lastFinished);
                }
            } else {
                lastFinished = false;
            }
        } else {
            writeShort(NULL_GROUP_TYPE);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType,
            MessageWriter writer) {
        if (arr != null) {
            int len = arr.length;

            if (arrPos == -1) {
                writeInt(len);

                if (!lastFinished) {
                    return;
                }

                arrPos = 0;
            }

            while (arrPos < len || arrCur != NULL) {
                if (arrCur == NULL) {
                    arrCur = arr[arrPos++];
                }

                write(itemType, arrCur, writer);

                if (!lastFinished) {
                    return;
                }

                arrCur = NULL;
            }

            arrPos = -1;
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType,
            MessageWriter writer) {
        if (col != null) {
            if (col instanceof List && col instanceof RandomAccess) {
                writeRandomAccessList((List<T>) col, itemType, writer);
            } else {
                if (it == null) {
                    writeInt(col.size());

                    if (!lastFinished) {
                        return;
                    }

                    it = col.iterator();
                }

                while (it.hasNext() || cur != NULL) {
                    if (cur == NULL) {
                        cur = it.next();
                    }

                    write(itemType, cur, writer);

                    if (!lastFinished) {
                        return;
                    }

                    cur = NULL;
                }

                it = null;
            }
        } else {
            writeInt(-1);
        }
    }

    @Override
    public <T> void writeSet(Set<T> set, MessageCollectionItemType itemType, MessageWriter writer) {
        writeCollection(set, itemType, writer);
    }

    /**
     * Writes {@link List}.
     *
     * @param list     List.
     * @param itemType Component type.
     * @param writer   Writer.
     */
    private <T> void writeRandomAccessList(List<T> list, MessageCollectionItemType itemType, MessageWriter writer) {
        assert list instanceof RandomAccess;

        int size = list.size();

        if (arrPos == -1) {
            writeInt(size);

            if (!lastFinished) {
                return;
            }

            arrPos = 0;
        }

        while (arrPos < size || arrCur != NULL) {
            if (arrCur == NULL) {
                arrCur = list.get(arrPos++);
            }

            write(itemType, arrCur, writer);

            if (!lastFinished) {
                return;
            }

            arrCur = NULL;
        }

        arrPos = -1;
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType,
            MessageCollectionItemType valType, MessageWriter writer) {
        if (map != null) {
            if (mapIt == null) {
                writeInt(map.size());

                if (!lastFinished) {
                    return;
                }

                mapIt = map.entrySet().iterator();
            }

            while (mapIt.hasNext() || mapCur != NULL) {
                if (mapCur == NULL) {
                    mapCur = mapIt.next();
                }

                Map.Entry<K, V> e = (Map.Entry<K, V>) mapCur;

                if (!keyDone) {
                    write(keyType, e.getKey(), writer);

                    if (!lastFinished) {
                        return;
                    }

                    keyDone = true;
                }

                write(valType, e.getValue(), writer);

                if (!lastFinished) {
                    return;
                }

                mapCur = NULL;
                keyDone = false;
            }

            mapIt = null;
        } else {
            writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte() {
        lastFinished = remainingInternal() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            setPosition(pos + 1);

            return GridUnsafe.getByte(heapArr, baseOff + pos);
        } else {
            return 0;
        }
    }

    @Override
    public @Nullable Byte readBoxedByte() {
        return readBoxedValue(this::readByte);
    }

    private <T> @Nullable T readBoxedValue(Supplier<T> valueReader) {
        // First, check if we have read the null flag in a previous call.
        if (boxedTypeNotNull || readBoolean()) {
            boxedTypeNotNull = true;

            T result = valueReader.get();

            // If the whole value has been read successfully, reset the state.
            if (lastFinished) {
                boxedTypeNotNull = false;
            }

            return result;
        } else {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() {
        lastFinished = false;

        short val = 0;

        int pos = buf.position();

        int limit = buf.limit();

        while (pos < limit) {
            byte b = GridUnsafe.getByte(heapArr, baseOff + pos);

            pos++;

            prim |= ((long) b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                lastFinished = true;

                val = (short) (prim - 1);

                prim = 0;
                primShift = 0;

                break;
            } else {
                primShift++;
            }
        }

        setPosition(pos);

        return val;
    }

    @Override
    public @Nullable Short readBoxedShort() {
        return readBoxedValue(this::readShort);
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() {
        lastFinished = false;

        int val = 0;

        int pos = buf.position();

        int limit = buf.limit();

        while (pos < limit) {
            byte b = GridUnsafe.getByte(heapArr, baseOff + pos);

            pos++;

            prim |= ((long) b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                lastFinished = true;

                val = (int) prim - 1;

                prim = 0;
                primShift = 0;

                break;
            } else {
                primShift++;
            }
        }

        setPosition(pos);

        return val;
    }

    @Override
    public @Nullable Integer readBoxedInt() {
        return readBoxedValue(this::readInt);
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() {
        lastFinished = false;

        long val = 0;

        int pos = buf.position();

        int limit = buf.limit();

        while (pos < limit) {
            byte b = GridUnsafe.getByte(heapArr, baseOff + pos);

            pos++;

            prim |= ((long) b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                lastFinished = true;

                val = prim - 1;

                prim = 0;
                primShift = 0;

                break;
            } else {
                primShift++;
            }
        }

        setPosition(pos);

        return val;
    }

    @Override
    public @Nullable Long readBoxedLong() {
        return readBoxedValue(this::readLong);
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() {
        lastFinished = remainingInternal() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            setPosition(pos + 4);

            long off = baseOff + pos;

            return IS_BIG_ENDIAN ? GridUnsafe.getFloatLittleEndian(heapArr, off) : GridUnsafe.getFloat(heapArr, off);
        } else {
            return 0;
        }
    }

    @Override
    public @Nullable Float readBoxedFloat() {
        return readBoxedValue(this::readFloat);
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() {
        lastFinished = remainingInternal() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            setPosition(pos + 8);

            long off = baseOff + pos;

            return IS_BIG_ENDIAN ? GridUnsafe.getDoubleLittleEndian(heapArr, off) : GridUnsafe.getDouble(heapArr, off);
        } else {
            return 0;
        }
    }

    @Override
    public @Nullable Double readBoxedDouble() {
        return readBoxedValue(this::readDouble);
    }

    /** {@inheritDoc} */
    @Override
    public char readChar() {
        lastFinished = remainingInternal() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            setPosition(pos + 2);

            long off = baseOff + pos;

            return IS_BIG_ENDIAN ? GridUnsafe.getCharLittleEndian(heapArr, off) : GridUnsafe.getChar(heapArr, off);
        } else {
            return 0;
        }
    }

    @Override
    public @Nullable Character readBoxedChar() {
        return readBoxedValue(this::readChar);
    }

    /** {@inheritDoc} */
    @Override
    public boolean readBoolean() {
        lastFinished = buf.hasRemaining();

        if (lastFinished) {
            int pos = buf.position();

            setPosition(pos + 1);

            return GridUnsafe.getBoolean(heapArr, baseOff + pos);
        } else {
            return false;
        }
    }

    @Override
    public @Nullable Boolean readBoxedBoolean() {
        return readBoxedValue(this::readBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] readByteArray() {
        return readArray(BYTE_ARRAY, 0, BYTE_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override
    public short[] readShortArray() {
        if (IS_BIG_ENDIAN) {
            return readArrayLittleEndian(SHORT_ARRAY, 2, 1, SHORT_ARR_OFF);
        } else {
            return readArray(SHORT_ARRAY, 1, SHORT_ARR_OFF);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int[] readIntArray() {
        if (IS_BIG_ENDIAN) {
            return readArrayLittleEndian(INT_ARRAY, 4, 2, INT_ARR_OFF);
        } else {
            return readArray(INT_ARRAY, 2, INT_ARR_OFF);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long[] readLongArray() {
        if (IS_BIG_ENDIAN) {
            return readArrayLittleEndian(LONG_ARRAY, 8, 3, LONG_ARR_OFF);
        } else {
            return readArray(LONG_ARRAY, 3, LONG_ARR_OFF);
        }
    }

    /** {@inheritDoc} */
    @Override
    public float[] readFloatArray() {
        if (IS_BIG_ENDIAN) {
            return readArrayLittleEndian(FLOAT_ARRAY, 4, 2, FLOAT_ARR_OFF);
        } else {
            return readArray(FLOAT_ARRAY, 2, FLOAT_ARR_OFF);
        }
    }

    /** {@inheritDoc} */
    @Override
    public double[] readDoubleArray() {
        if (IS_BIG_ENDIAN) {
            return readArrayLittleEndian(DOUBLE_ARRAY, 8, 3, DOUBLE_ARR_OFF);
        } else {
            return readArray(DOUBLE_ARRAY, 3, DOUBLE_ARR_OFF);
        }
    }

    /** {@inheritDoc} */
    @Override
    public char[] readCharArray() {
        if (IS_BIG_ENDIAN) {
            return readArrayLittleEndian(CHAR_ARRAY, 2, 1, CHAR_ARR_OFF);
        } else {
            return readArray(CHAR_ARRAY, 1, CHAR_ARR_OFF);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean[] readBooleanArray() {
        return readArray(BOOLEAN_ARRAY, 0, GridUnsafe.BOOLEAN_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override
    public String readString() {
        byte[] arr = readByteArray();

        return arr != null ? new String(arr, UTF_8) : null;
    }

    /** {@inheritDoc} */
    @Override
    public BitSet readBitSet() {
        long[] arr = readLongArray();

        return arr != null ? BitSet.valueOf(arr) : null;
    }

    @Override
    public ByteBuffer readByteBuffer() {
        byte[] bytes;

        switch (byteBufferState) {
            case 0:
                byteBufferFlag = readByte();

                boolean isNull = (byteBufferFlag & BYTE_BUFFER_NOT_NULL_FLAG) == 0;

                if (!lastFinished || isNull) {
                    return null;
                }

                byteBufferState++;

                //noinspection fallthrough
            case 1:
                bytes = readByteArray();

                if (!lastFinished) {
                    return null;
                }

                byteBufferState = 0;
                break;

            default:
                throw new IllegalArgumentException("Unknown byteBufferState: " + byteBufferState);
        }

        ByteBuffer val = ByteBuffer.wrap(bytes);

        if ((byteBufferFlag & BYTE_BUFFER_BIG_ENDIAN_FLAG) == 0) {
            val.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            val.order(ByteOrder.BIG_ENDIAN);
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public UUID readUuid() {
        switch (uuidState) {
            case 0:
                boolean isNull = readBoolean();

                if (!lastFinished || isNull) {
                    return null;
                }

                uuidState++;

                //noinspection fallthrough
            case 1:
                lastFinished = remainingInternal() >= 2 * Long.BYTES;

                if (lastFinished) {
                    int pos = buf.position();

                    long msb = GridUnsafe.getLongLittleEndian(heapArr, baseOff + pos);
                    long lsb = GridUnsafe.getLongLittleEndian(heapArr, baseOff + pos + Long.BYTES);

                    setPosition(pos + 2 * Long.BYTES);

                    uuidState = 0;

                    return new UUID(msb, lsb);
                }
                break;

            default:
                throw new IllegalArgumentException("Unknown uuidState: " + uuidState);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteUuid readIgniteUuid() {
        switch (uuidState) {
            case 0:
                boolean isNull = readBoolean();

                if (!lastFinished || isNull) {
                    return null;
                }

                uuidState++;

                //noinspection fallthrough
            case 1:
                uuidMost = readLong();

                if (!lastFinished) {
                    return null;
                }

                uuidState++;

                //noinspection fallthrough
            case 2:
                uuidLeast = readLong();

                if (!lastFinished) {
                    return null;
                }

                uuidState++;

                //noinspection fallthrough
            case 3:
                uuidLocId = readLong();

                if (!lastFinished) {
                    return null;
                }

                uuidState = 0;
                break;

            default:
                throw new IllegalArgumentException("Unknown uuidState: " + uuidState);
        }

        final IgniteUuid val = new IgniteUuid(new UUID(uuidMost, uuidLeast), uuidLocId);

        uuidMost = 0;
        uuidLeast = 0;
        uuidLocId = 0;

        return val;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public <T extends NetworkMessage> T readMessage(MessageReader reader) {
        // If the deserializer is null then we haven't finished reading the message header.
        if (msgDeserializer == null) {
            // Read the message group type.
            if (!msgGroupTypeRead) {
                msgGroupType = readShort();

                if (!lastFinished) {
                    return null;
                }

                // Message group type will be equal to NetworkMessage.NULL_GROUP_TYPE if a nested message is null.
                if (msgGroupType == NULL_GROUP_TYPE) { // "lastFinished" is "true" here, so no further parsing will be required.
                    return null;
                }

                // Save current progress, because we can read the header in two chunks.
                msgGroupTypeRead = true;
            }

            // Read the message type.
            short msgType = readShort();

            if (!lastFinished) {
                return null;
            }

            msgDeserializer = serializationRegistry.createDeserializer(msgGroupType, msgType);
        }

        // If the deserializer is not null then we have definitely finished parsing the header and can read the message
        // body.
        reader.beforeInnerMessageRead();

        try {
            reader.setCurrentReadClass(msgDeserializer.klass());

            reader.setBuffer(buf);

            lastFinished = msgDeserializer.readMessage(reader);
        } finally {
            reader.afterInnerMessageRead(lastFinished);
        }

        if (lastFinished) {
            T result = (T) msgDeserializer.getMessage();

            msgGroupTypeRead = false;
            msgDeserializer = null;

            return result;
        } else {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> T[] readObjectArray(MessageCollectionItemType itemType, Class<T> itemCls,
            MessageReader reader) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished) {
                return null;
            }

            readSize = size;
        }

        if (readSize >= 0) {
            if (objArr == null) {
                objArr = itemCls != null ? (Object[]) Array.newInstance(itemCls, readSize) : new Object[readSize];
            }

            for (int i = readItems; i < readSize; i++) {
                Object item = read(itemType, reader);

                if (!lastFinished) {
                    return null;
                }

                objArr[i] = item;

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;
        cur = null;

        T[] objArr0 = (T[]) objArr;

        objArr = null;

        return objArr0;
    }

    /** {@inheritDoc} */
    @Override
    public <C extends Collection<?>> C readCollection(MessageCollectionItemType itemType,
            MessageReader reader) {
        return (C) readCollection0(itemType, reader, ArrayList::new);
    }

    @Override
    public <C extends Set<?>> C readSet(MessageCollectionItemType itemType, MessageReader reader) {
        return (C) readCollection0(itemType, reader, HashSet::new);
    }

    /**
     * Common implementation for {@link #readCollection(MessageCollectionItemType, MessageReader)} and
     * {@link #readSet(MessageCollectionItemType, MessageReader)}. Reads a sequence of objects and puts it into a collection, created by
     * {@code ctor}.
     *
     * @param itemType Collection item type.
     * @param reader Message reader instance.
     * @param ctor Factory for creating a collection using its length.
     *
     * @return Collection, read from the reader, or {@code null} if reading has not completed yet.
     */
    private @Nullable Collection<?> readCollection0(
            MessageCollectionItemType itemType,
            MessageReader reader,
            IntFunction<Collection<Object>> ctor
    ) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished) {
                return null;
            }

            readSize = size;
        }

        if (readSize >= 0) {
            if (col == null) {
                col = ctor.apply(readSize);
            }

            for (int i = readItems; i < readSize; i++) {
                Object item = read(itemType, reader);

                if (!lastFinished) {
                    return null;
                }

                col.add(item);

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;
        cur = null;

        Collection<?> col0 = col;

        col = null;

        return col0;
    }

    /** {@inheritDoc} */
    @Override
    public <M extends Map<?, ?>> M readMap(MessageCollectionItemType keyType,
            MessageCollectionItemType valType, boolean linked, MessageReader reader) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished) {
                return null;
            }

            readSize = size;
        }

        if (readSize >= 0) {
            if (map == null) {
                map = linked ? IgniteUtils.newLinkedHashMap(readSize) : IgniteUtils.newHashMap(readSize);
            }

            for (int i = readItems; i < readSize; i++) {
                if (!keyDone) {
                    Object key = read(keyType, reader);

                    if (!lastFinished) {
                        return null;
                    }

                    mapCur = key;
                    keyDone = true;
                }

                Object val = read(valType, reader);

                if (!lastFinished) {
                    return null;
                }

                map.put(mapCur, val);

                keyDone = false;

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;
        mapCur = null;

        M map0 = (M) map;

        map = null;

        return map0;
    }

    /**
     * Writes array.
     *
     * @param arr   Array.
     * @param off   Offset.
     * @param len   Length.
     * @param bytes Length in bytes.
     * @return Whether array was fully written.
     */
    boolean writeArray(@Nullable Object arr, long off, int len, int bytes) {
        assert arr == null || arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();
        assert off > 0;
        assert len >= 0;
        assert bytes >= 0;
        assert bytes >= arrOff;

        if (writeArrayLength(len)) {
            return false;
        }

        int toWrite = bytes - arrOff;
        int pos = buf.position();
        int remaining = remainingInternal();

        if (toWrite <= remaining) {
            if (toWrite > 0) {
                GridUnsafe.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, toWrite);

                setPosition(pos + toWrite);
            }

            arrOff = -1;

            return true;
        } else {
            if (remaining > 0) {
                GridUnsafe.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, remaining);

                setPosition(pos + remaining);

                arrOff += remaining;
            }

            return false;
        }
    }

    /**
     * Writes array.
     *
     * @param arr      Array.
     * @param off      Offset.
     * @param len      Length.
     * @param typeSize Primitive type size in bytes. Needs for byte reverse.
     * @param shiftCnt Shift for length.
     * @return Whether array was fully written.
     */
    boolean writeArrayLittleEndian(Object arr, long off, int len, int typeSize, int shiftCnt) {
        assert arr != null;
        assert arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();
        assert off > 0;
        assert len >= 0;

        int bytes = len << shiftCnt;

        assert bytes >= arrOff;

        if (writeArrayLength(len)) {
            return false;
        }

        int toWrite = (bytes - arrOff) >> shiftCnt;
        int remaining = remainingInternal() >> shiftCnt;

        if (toWrite <= remaining) {
            writeArrayLittleEndian(arr, off, toWrite, typeSize);

            arrOff = -1;

            return true;
        } else {
            if (remaining > 0) {
                writeArrayLittleEndian(arr, off, remaining, typeSize);
            }

            return false;
        }
    }

    /**
     * Writes array.
     *
     * @param arr      Array.
     * @param off      Offset.
     * @param len      Length.
     * @param typeSize Primitive type size in bytes.
     */
    private void writeArrayLittleEndian(Object arr, long off, int len, int typeSize) {
        int pos = buf.position();

        for (int i = 0; i < len; i++) {
            for (int j = 0; j < typeSize; j++) {
                byte b = GridUnsafe.getByteField(arr, off + arrOff + (typeSize - j - 1));

                GridUnsafe.putByte(heapArr, baseOff + pos++, b);
            }

            setPosition(pos);
            arrOff += typeSize;
        }
    }

    /**
     * Writes array length.
     *
     * @param len Length.
     */
    private boolean writeArrayLength(int len) {
        if (arrOff == -1) {
            writeInt(len);

            if (!lastFinished) {
                return true;
            }

            arrOff = 0;
        }
        return false;
    }

    /**
     * Reads array.
     *
     * @param <T>      Type of array.
     * @param creator  Array creator.
     * @param lenShift Array length shift size.
     * @param off      Base offset.
     * @return Array or special value if it was not fully read.
     */
    <T> T readArray(ArrayFactory<T> creator, int lenShift, long off) {
        assert creator != null;

        if (tmpArr == null) {
            int len = readInt();

            if (!lastFinished) {
                return null;
            }

            switch (len) {
                case -1:
                    lastFinished = true;

                    return null;

                case 0:
                    lastFinished = true;

                    return creator.of(0);

                default:
                    tmpArr = creator.of(len);
                    tmpArrBytes = len << lenShift;
            }
        }

        int toRead = tmpArrBytes - tmpArrOff;
        int remaining = remainingInternal();
        int pos = buf.position();

        lastFinished = toRead <= remaining;

        if (lastFinished) {
            GridUnsafe.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, toRead);

            setPosition(pos + toRead);

            final T arr = (T) tmpArr;

            tmpArr = null;
            tmpArrBytes = 0;
            tmpArrOff = 0;

            return arr;
        } else {
            GridUnsafe.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, remaining);

            setPosition(pos + remaining);

            tmpArrOff += remaining;

            return null;
        }
    }

    /**
     * Reads array.
     *
     * @param <T>      Type of array.
     * @param creator  Array creator.
     * @param typeSize Primitive type size in bytes.
     * @param lenShift Array length shift size.
     * @param off      Base offset.
     * @return Array or special value if it was not fully read.
     */
    <T> T readArrayLittleEndian(ArrayFactory<T> creator, int typeSize, int lenShift, long off) {
        assert creator != null;

        if (tmpArr == null) {
            int len = readInt();

            if (!lastFinished) {
                return null;
            }

            switch (len) {
                case -1:
                    lastFinished = true;

                    return null;

                case 0:
                    lastFinished = true;

                    return creator.of(0);

                default:
                    tmpArr = creator.of(len);
                    tmpArrBytes = len << lenShift;
            }
        }

        int toRead = tmpArrBytes - tmpArrOff - valReadBytes;
        int remaining = remainingInternal();

        lastFinished = toRead <= remaining;

        if (!lastFinished) {
            toRead = remaining;
        }

        int pos = buf.position();

        for (int i = 0; i < toRead; i++) {
            byte b = GridUnsafe.getByte(heapArr, baseOff + pos + i);

            GridUnsafe.putByteField(tmpArr, off + tmpArrOff + (typeSize - valReadBytes - 1), b);

            if (++valReadBytes == typeSize) {
                valReadBytes = 0;
                tmpArrOff += typeSize;
            }
        }

        setPosition(pos + toRead);

        if (lastFinished) {
            final T arr = (T) tmpArr;

            tmpArr = null;
            tmpArrBytes = 0;
            tmpArrOff = 0;

            return arr;
        } else {
            return null;
        }
    }

    /**
     * Writes value.
     *
     * @param type Type.
     * @param val Value.
     * @param writer Writer.
     */
    protected void write(MessageCollectionItemType type, Object val, MessageWriter writer) {
        switch (type) {
            case BYTE:
                writeByte((Byte) val);

                break;

            case SHORT:
                writeShort((Short) val);

                break;

            case INT:
                writeInt((Integer) val);

                break;

            case LONG:
                writeLong((Long) val);

                break;

            case FLOAT:
                writeFloat((Float) val);

                break;

            case DOUBLE:
                writeDouble((Double) val);

                break;

            case CHAR:
                writeChar((Character) val);

                break;

            case BOOLEAN:
                writeBoolean((Boolean) val);

                break;

            case BYTE_ARR:
                writeByteArray((byte[]) val);

                break;

            case SHORT_ARR:
                writeShortArray((short[]) val);

                break;

            case INT_ARR:
                writeIntArray((int[]) val);

                break;

            case LONG_ARR:
                writeLongArray((long[]) val);

                break;

            case FLOAT_ARR:
                writeFloatArray((float[]) val);

                break;

            case DOUBLE_ARR:
                writeDoubleArray((double[]) val);

                break;

            case CHAR_ARR:
                writeCharArray((char[]) val);

                break;

            case BOOLEAN_ARR:
                writeBooleanArray((boolean[]) val);

                break;

            case STRING:
                writeString((String) val);

                break;

            case BIT_SET:
                writeBitSet((BitSet) val);

                break;

            case BYTE_BUFFER:
                writeByteBuffer((ByteBuffer) val);

                break;

            case UUID:
                writeUuid((UUID) val);

                break;

            case IGNITE_UUID:
                writeIgniteUuid((IgniteUuid) val);

                break;

            case MSG:
                writeMessage((NetworkMessage) val, writer);

                break;

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Reads value.
     *
     * @param type   Type.
     * @param reader Reader.
     * @return Value.
     */
    protected Object read(MessageCollectionItemType type, MessageReader reader) {
        switch (type) {
            case BYTE:
                return readByte();

            case SHORT:
                return readShort();

            case INT:
                return readInt();

            case LONG:
                return readLong();

            case FLOAT:
                return readFloat();

            case DOUBLE:
                return readDouble();

            case CHAR:
                return readChar();

            case BOOLEAN:
                return readBoolean();

            case BYTE_ARR:
                return readByteArray();

            case SHORT_ARR:
                return readShortArray();

            case INT_ARR:
                return readIntArray();

            case LONG_ARR:
                return readLongArray();

            case FLOAT_ARR:
                return readFloatArray();

            case DOUBLE_ARR:
                return readDoubleArray();

            case CHAR_ARR:
                return readCharArray();

            case BOOLEAN_ARR:
                return readBooleanArray();

            case STRING:
                return readString();

            case BIT_SET:
                return readBitSet();

            case BYTE_BUFFER:
                return readByteBuffer();

            case UUID:
                return readUuid();

            case IGNITE_UUID:
                return readIgniteUuid();

            case MSG:
                return readMessage(reader);

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
}
