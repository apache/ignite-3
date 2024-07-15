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

package org.apache.ignite.internal.network.direct;

import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.util.ArrayUtils.EMPTY_BYTE_BUFFER;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.direct.state.DirectMessageState;
import org.apache.ignite.internal.network.direct.state.DirectMessageStateItem;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStreamImplV1;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.jetbrains.annotations.Nullable;

/**
 * Message writer implementation.
 */
public class DirectMessageWriter implements MessageWriter {
    /** State. */
    private final DirectMessageState<StateItem> state;

    /** We store this field pre-cached to avoid frequent {@link DirectMessageState#item()} calls. */
    private StateItem stateItem;

    /** We store this field pre-cached to avoid frequent {@link StateItem#stream} dereferencing. */
    private DirectByteBufferStream stream;

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization registry.
     * @param protoVer Protocol version.
     */
    public DirectMessageWriter(MessageSerializationRegistry serializationRegistry, byte protoVer) {
        state = new DirectMessageState<>(StateItem.class, () -> new StateItem(createStream(serializationRegistry, protoVer)));

        stateItem = state.item();
        stream = stateItem.stream;
    }

    /** {@inheritDoc} */
    @Override
    public void setBuffer(ByteBuffer buf) {
        this.stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override
    public void setCurrentWriteClass(Class<? extends NetworkMessage> msgCls) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeHeader(short groupType, short messageType, byte fieldCnt) {
        DirectByteBufferStream stream = this.stream;

        // first part of the header might have already been sent in a previous write attempt
        if (!stateItem.partialHdrWritten) {
            stream.writeShort(groupType);

            if (stream.lastFinished()) {
                stateItem.partialHdrWritten = true;
            } else {
                return false;
            }
        }

        stream.writeShort(messageType);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeByte(String name, byte val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeByte(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedByte(String name, @Nullable Byte val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedByte(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeShort(String name, short val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeShort(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedShort(String name, @Nullable Short val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedShort(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeInt(String name, int val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeInt(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedInt(String name, @Nullable Integer val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedInt(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeLong(String name, long val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeLong(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedLong(String name, @Nullable Long val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedLong(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeFloat(String name, float val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeFloat(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedFloat(String name, @Nullable Float val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedFloat(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeDouble(String name, double val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeDouble(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedDouble(String name, @Nullable Double val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedDouble(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeChar(String name, char val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeChar(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedChar(String name, @Nullable Character val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedChar(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeBoolean(String name, boolean val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoolean(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeBoxedBoolean(String name, @Nullable Boolean val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBoxedBoolean(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeByteArray(String name, @Nullable byte[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeByteArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeByteArray(String name, byte[] val, long off, int len) {
        DirectByteBufferStream stream = this.stream;

        stream.writeByteArray(val, off, len);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeShortArray(String name, @Nullable short[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeShortArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeIntArray(String name, @Nullable int[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeIntArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeLongArray(String name, @Nullable long[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeLongArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeLongArray(String name, long[] val, int len) {
        DirectByteBufferStream stream = this.stream;

        stream.writeLongArray(val, len);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeFloatArray(String name, @Nullable float[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeFloatArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeDoubleArray(String name, @Nullable double[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeDoubleArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeCharArray(String name, @Nullable char[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeCharArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeBooleanArray(String name, @Nullable boolean[] val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBooleanArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeString(String name, String val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeString(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeBitSet(String name, BitSet val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeBitSet(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeByteBuffer(String name, ByteBuffer val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeByteBuffer(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeUuid(String name, UUID val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeUuid(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeIgniteUuid(String name, IgniteUuid val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeIgniteUuid(val);

        return stream.lastFinished();
    }

    @Override
    public boolean writeHybridTimestamp(String name, @Nullable HybridTimestamp val) {
        DirectByteBufferStream stream = this.stream;

        stream.writeLong(val == null ? NULL_HYBRID_TIMESTAMP : val.longValue());

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeMessage(String name, @Nullable NetworkMessage msg) {
        DirectByteBufferStream stream = this.stream;

        stream.writeMessage(msg, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public <T> boolean writeObjectArray(String name, T[] arr, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = this.stream;

        stream.writeObjectArray(arr, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public <T> boolean writeCollection(String name, Collection<T> col, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = this.stream;

        stream.writeCollection(col, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public <T> boolean writeList(String name, List<T> col, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = this.stream;

        stream.writeCollection(col, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public <T> boolean writeSet(String name, Set<T> col, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = this.stream;

        stream.writeSet(col, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> boolean writeMap(String name, Map<K, V> map, MessageCollectionItemType keyType,
            MessageCollectionItemType valType) {
        DirectByteBufferStream stream = this.stream;

        stream.writeMap(map, keyType, valType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isHeaderWritten() {
        return stateItem.hdrWritten;
    }

    /** {@inheritDoc} */
    @Override
    public void onHeaderWritten() {
        stateItem.hdrWritten = true;
    }

    /** {@inheritDoc} */
    @Override
    public int state() {
        return stateItem.state;
    }

    /** {@inheritDoc} */
    @Override
    public void incrementState() {
        stateItem.state++;
    }

    /** {@inheritDoc} */
    @Override
    public void beforeInnerMessageWrite() {
        state.forward();
        stateItem = state.item();
        stream = stateItem.stream;
    }

    /** {@inheritDoc} */
    @Override
    public void afterInnerMessageWrite(boolean finished) {
        // Prevent memory leaks.
        setBuffer(EMPTY_BYTE_BUFFER);

        state.backward(finished);
        stateItem = state.item();
        stream = stateItem.stream;
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        state.reset();
        stateItem = state.item();
        stream = stateItem.stream;
    }

    /**
     * Returns a stream to write message fields recursively.
     *
     * @param serializationRegistry Serialization registry.
     * @param protoVer Protocol version.
     */
    protected DirectByteBufferStream createStream(MessageSerializationRegistry serializationRegistry, byte protoVer) {
        switch (protoVer) {
            case 1:
                return new DirectByteBufferStreamImplV1(serializationRegistry);

            default:
                throw new IllegalStateException("Invalid protocol version: " + protoVer);
        }
    }

    /**
     * State item.
     */
    private static class StateItem implements DirectMessageStateItem {
        final DirectByteBufferStream stream;

        int state;

        /**
         * Flag indicating that the first part of the message header has been written.
         */
        boolean partialHdrWritten;

        /**
         * Flag indicating that the whole message header has been written.
         */
        boolean hdrWritten;

        /**
         * Constructor.
         *
         * @param stream Direct byte buffer stream.
         */
        StateItem(DirectByteBufferStream stream) {
            this.stream = stream;
        }

        /** {@inheritDoc} */
        @Override
        public void reset() {
            state = 0;
            partialHdrWritten = false;
            hdrWritten = false;
        }
    }
}
