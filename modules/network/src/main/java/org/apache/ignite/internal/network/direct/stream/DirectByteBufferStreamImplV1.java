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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.UUID;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.util.ArrayFactory;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageReader;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.apache.ignite.network.serialization.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.jetbrains.annotations.Nullable;

/**
 * {@link DirectByteBufferStream} implementation.
 */
public class DirectByteBufferStreamImplV1 implements DirectByteBufferStream {
    /** Poison object. */
    private static final Object NULL = new Object();

    /** Flag that indicates that byte buffer is not null. */
    private static final byte BYTE_BUFFER_NOT_NULL_FLAG = 1;

    /** Flag that indicates that byte buffer has Big Endinan order. */
    private static final byte BYTE_BUFFER_BIG_ENDIAN_FLAG = 2;

    /** Message serialization registry. */
    private final PerSessionSerializationService serializationService;

    private ByteBuffer buf;

    private byte[] heapArr;

    private long baseOff;

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
     * @param serializationService Serialization service.       .
     */
    public DirectByteBufferStreamImplV1(PerSessionSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    /** {@inheritDoc} */
    @Override
    public void setBuffer(ByteBuffer buf) {
        assert buf != null;

        if (this.buf != buf) {
            this.buf = buf;

            heapArr = buf.isDirect() ? null : buf.array();
            baseOff = buf.isDirect() ? GridUnsafe.bufferAddress(buf) : BYTE_ARR_OFF;
        }
    }

    /** {@inheritDoc} */
    @Override
    public int remaining() {
        return buf.remaining();
    }

    /** {@inheritDoc} */
    @Override
    public boolean lastFinished() {
        return lastFinished;
    }

    /** {@inheritDoc} */
    @Override
    public void writeByte(byte val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            GridUnsafe.putByte(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeShort(short val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (IS_BIG_ENDIAN) {
                GridUnsafe.putShortLittleEndian(heapArr, off, val);
            } else {
                GridUnsafe.putShort(heapArr, off, val);
            }

            buf.position(pos + 2);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeInt(int val) {
        lastFinished = buf.remaining() >= 5;

        if (lastFinished) {
            if (val == Integer.MAX_VALUE) {
                val = Integer.MIN_VALUE;
            } else {
                val++;
            }

            int pos = buf.position();

            while ((val & 0xFFFF_FF80) != 0) {
                byte b = (byte) (val | 0x80);

                GridUnsafe.putByte(heapArr, baseOff + pos++, b);

                val >>>= 7;
            }

            GridUnsafe.putByte(heapArr, baseOff + pos++, (byte) val);

            buf.position(pos);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeLong(long val) {
        lastFinished = buf.remaining() >= 10;

        if (lastFinished) {
            if (val == Long.MAX_VALUE) {
                val = Long.MIN_VALUE;
            } else {
                val++;
            }

            int pos = buf.position();

            while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
                byte b = (byte) (val | 0x80);

                GridUnsafe.putByte(heapArr, baseOff + pos++, b);

                val >>>= 7;
            }

            GridUnsafe.putByte(heapArr, baseOff + pos++, (byte) val);

            buf.position(pos);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloat(float val) {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (IS_BIG_ENDIAN) {
                GridUnsafe.putFloatLittleEndian(heapArr, off, val);
            } else {
                GridUnsafe.putFloat(heapArr, off, val);
            }

            buf.position(pos + 4);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeDouble(double val) {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (IS_BIG_ENDIAN) {
                GridUnsafe.putDoubleLittleEndian(heapArr, off, val);
            } else {
                GridUnsafe.putDouble(heapArr, off, val);
            }

            buf.position(pos + 8);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeChar(char val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (IS_BIG_ENDIAN) {
                GridUnsafe.putCharLittleEndian(heapArr, off, val);
            } else {
                GridUnsafe.putChar(heapArr, off, val);
            }

            buf.position(pos + 2);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeBoolean(boolean val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            GridUnsafe.putBoolean(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
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
                throw new IllegalArgumentException("Unknown byteBufferState: " + uuidState);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeUuid(UUID val) {
        switch (uuidState) {
            case 0:
                writeBoolean(val == null);

                if (!lastFinished || val == null) {
                    return;
                }

                uuidState++;

                //noinspection fallthrough
            case 1:
                writeLong(val.getMostSignificantBits());

                if (!lastFinished) {
                    return;
                }

                uuidState++;

                //noinspection fallthrough
            case 2:
                writeLong(val.getLeastSignificantBits());

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
                        msgSerializer = serializationService.createMessageSerializer(msg.groupType(), msg.messageType());
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
            writeShort(Short.MIN_VALUE);
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
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 1);

            return GridUnsafe.getByte(heapArr, baseOff + pos);
        } else {
            return 0;
        }
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 2);

            long off = baseOff + pos;

            return IS_BIG_ENDIAN ? GridUnsafe.getShortLittleEndian(heapArr, off) : GridUnsafe.getShort(heapArr, off);
        } else {
            return 0;
        }
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() {
        lastFinished = false;

        int val = 0;

        while (buf.hasRemaining()) {
            int pos = buf.position();

            byte b = GridUnsafe.getByte(heapArr, baseOff + pos);

            buf.position(pos + 1);

            prim |= ((long) b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                lastFinished = true;

                val = (int) prim;

                if (val == Integer.MIN_VALUE) {
                    val = Integer.MAX_VALUE;
                } else {
                    val--;
                }

                prim = 0;
                primShift = 0;

                break;
            } else {
                primShift++;
            }
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() {
        lastFinished = false;

        long val = 0;

        while (buf.hasRemaining()) {
            int pos = buf.position();

            byte b = GridUnsafe.getByte(heapArr, baseOff + pos);

            buf.position(pos + 1);

            prim |= ((long) b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                lastFinished = true;

                val = prim;

                if (val == Long.MIN_VALUE) {
                    val = Long.MAX_VALUE;
                } else {
                    val--;
                }

                prim = 0;
                primShift = 0;

                break;
            } else {
                primShift++;
            }
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 4);

            long off = baseOff + pos;

            return IS_BIG_ENDIAN ? GridUnsafe.getFloatLittleEndian(heapArr, off) : GridUnsafe.getFloat(heapArr, off);
        } else {
            return 0;
        }
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 8);

            long off = baseOff + pos;

            return IS_BIG_ENDIAN ? GridUnsafe.getDoubleLittleEndian(heapArr, off) : GridUnsafe.getDouble(heapArr, off);
        } else {
            return 0;
        }
    }

    /** {@inheritDoc} */
    @Override
    public char readChar() {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 2);

            long off = baseOff + pos;

            return IS_BIG_ENDIAN ? GridUnsafe.getCharLittleEndian(heapArr, off) : GridUnsafe.getChar(heapArr, off);
        } else {
            return 0;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean readBoolean() {
        lastFinished = buf.hasRemaining();

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 1);

            return GridUnsafe.getBoolean(heapArr, baseOff + pos);
        } else {
            return false;
        }
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
                throw new IllegalArgumentException("Unknown byteBufferState: " + uuidState);
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

                uuidState = 0;
                break;

            default:
                throw new IllegalArgumentException("Unknown uuidState: " + uuidState);
        }

        UUID val = new UUID(uuidMost, uuidLeast);

        uuidMost = 0;
        uuidLeast = 0;

        return val;
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
        // if the deserialzer is null then we haven't finished reading the message header
        if (msgDeserializer == null) {
            // read the message group type
            if (!msgGroupTypeRead) {
                msgGroupType = readShort();

                if (!lastFinished) {
                    return null;
                }

                // message group type will be equal to Short.MIN_VALUE if a nested message is null
                if (msgGroupType == Short.MIN_VALUE) { // lastFinished is "true" here, so no further parsing will be required
                    return null;
                }

                // save current progress, because we can read the header in two chunks
                msgGroupTypeRead = true;
            }

            // read the message type
            short msgType = readShort();

            if (!lastFinished) {
                return null;
            }

            msgDeserializer = serializationService.createMessageDeserializer(msgGroupType, msgType);
        }

        // if the deserializer is not null then we have definitely finished parsing the header and can read the message
        // body
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
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished) {
                return null;
            }

            readSize = size;
        }

        if (readSize >= 0) {
            if (col == null) {
                col = new ArrayList<>(readSize);
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

        C col0 = (C) col;

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
        int remaining = buf.remaining();

        if (toWrite <= remaining) {
            if (toWrite > 0) {
                GridUnsafe.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, toWrite);

                buf.position(pos + toWrite);
            }

            arrOff = -1;

            return true;
        } else {
            if (remaining > 0) {
                GridUnsafe.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, remaining);

                buf.position(pos + remaining);

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
        int remaining = buf.remaining() >> shiftCnt;

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

            buf.position(pos);
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
        int remaining = buf.remaining();
        int pos = buf.position();

        lastFinished = toRead <= remaining;

        if (lastFinished) {
            GridUnsafe.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, toRead);

            buf.position(pos + toRead);

            final T arr = (T) tmpArr;

            tmpArr = null;
            tmpArrBytes = 0;
            tmpArrOff = 0;

            return arr;
        } else {
            GridUnsafe.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, remaining);

            buf.position(pos + remaining);

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
        int remaining = buf.remaining();

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

        buf.position(pos + toRead);

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
                try {
                    if (val != null) {
                        writer.beforeInnerMessageWrite();
                    }

                    writeMessage((NetworkMessage) val, writer);
                } finally {
                    if (val != null) {
                        writer.afterInnerMessageWrite(lastFinished);
                    }
                }

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
