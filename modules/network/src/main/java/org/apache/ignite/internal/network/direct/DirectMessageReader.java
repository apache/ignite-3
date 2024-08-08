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

import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

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
import org.apache.ignite.internal.network.serialization.MessageReader;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.jetbrains.annotations.Nullable;

/**
 * Message reader implementation.
 */
public class DirectMessageReader implements MessageReader {
    /** State. */
    private final DirectMessageState<StateItem> state;

    /** Whether last field was fully read. */
    private boolean lastRead;

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization registry.
     * @param protoVer Protocol version.
     */
    public DirectMessageReader(
            MessageSerializationRegistry serializationRegistry,
            byte protoVer
    ) {
        state = new DirectMessageState<>(StateItem.class, () -> new StateItem(createStream(serializationRegistry, protoVer)));
    }

    /** {@inheritDoc} */
    @Override
    public void setBuffer(ByteBuffer buf) {
        state.item().stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override
    public void setCurrentReadClass(Class<? extends NetworkMessage> msgCls) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public boolean beforeMessageRead() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean afterMessageRead(Class<? extends NetworkMessage> msgCls) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte(String name) {
        DirectByteBufferStream stream = state.item().stream;

        byte val = stream.readByte();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable Byte readBoxedByte(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Byte val = stream.readBoxedByte();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public short readShort(String name) {
        DirectByteBufferStream stream = state.item().stream;

        short val = stream.readShort();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable Short readBoxedShort(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Short val = stream.readBoxedShort();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public int readInt(String name) {
        DirectByteBufferStream stream = state.item().stream;

        int val = stream.readInt();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public int readInt(String name, int dflt) {
        return readInt(name);
    }

    @Override
    public @Nullable Integer readBoxedInt(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Integer val = stream.readBoxedInt();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public long readLong(String name) {
        DirectByteBufferStream stream = state.item().stream;

        long val = stream.readLong();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable Long readBoxedLong(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Long val = stream.readBoxedLong();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat(String name) {
        DirectByteBufferStream stream = state.item().stream;

        float val = stream.readFloat();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable Float readBoxedFloat(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Float val = stream.readBoxedFloat();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble(String name) {
        DirectByteBufferStream stream = state.item().stream;

        double val = stream.readDouble();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable Double readBoxedDouble(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Double val = stream.readBoxedDouble();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public char readChar(String name) {
        DirectByteBufferStream stream = state.item().stream;

        char val = stream.readChar();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable Character readBoxedChar(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Character val = stream.readBoxedChar();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public boolean readBoolean(String name) {
        DirectByteBufferStream stream = state.item().stream;

        boolean val = stream.readBoolean();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable Boolean readBoxedBoolean(String name) {
        DirectByteBufferStream stream = state.item().stream;

        Boolean val = stream.readBoxedBoolean();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public byte[] readByteArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        byte[] arr = stream.readByteArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public short[] readShortArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        short[] arr = stream.readShortArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public int[] readIntArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        int[] arr = stream.readIntArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public long[] readLongArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        long[] arr = stream.readLongArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public float[] readFloatArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        float[] arr = stream.readFloatArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public double[] readDoubleArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        double[] arr = stream.readDoubleArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public char[] readCharArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        char[] arr = stream.readCharArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public boolean[] readBooleanArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        boolean[] arr = stream.readBooleanArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Override
    public String readString(String name) {
        DirectByteBufferStream stream = state.item().stream;

        String val = stream.readString();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public BitSet readBitSet(String name) {
        DirectByteBufferStream stream = state.item().stream;

        BitSet val = stream.readBitSet();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public ByteBuffer readByteBuffer(String name) {
        DirectByteBufferStream stream = state.item().stream;

        ByteBuffer val = stream.readByteBuffer();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public UUID readUuid(String name) {
        DirectByteBufferStream stream = state.item().stream;

        UUID val = stream.readUuid();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteUuid readIgniteUuid(String name) {
        DirectByteBufferStream stream = state.item().stream;

        IgniteUuid val = stream.readIgniteUuid();

        lastRead = stream.lastFinished();

        return val;
    }

    @Override
    public @Nullable HybridTimestamp readHybridTimestamp(String name) {
        DirectByteBufferStream stream = state.item().stream;

        long val = stream.readLong();

        lastRead = stream.lastFinished();

        if (!lastRead) {
            return null;
        }

        return nullableHybridTimestamp(val);
    }

    @Override
    public short readHeaderShort() {
        DirectByteBufferStream stream = state.item().stream;

        short val = stream.readShort();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public <T extends NetworkMessage> T readMessage(String name) {
        DirectByteBufferStream stream = state.item().stream;

        T msg = stream.readMessage(this);

        lastRead = stream.lastFinished();

        return msg;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T[] readObjectArray(String name, MessageCollectionItemType itemType, Class<T> itemCls) {
        DirectByteBufferStream stream = state.item().stream;

        T[] msg = stream.readObjectArray(itemType, itemCls, this);

        lastRead = stream.lastFinished();

        return msg;
    }

    /** {@inheritDoc} */
    @Override
    public <C extends Collection<?>> C readCollection(String name, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = state.item().stream;

        C col = stream.readCollection(itemType, this);

        lastRead = stream.lastFinished();

        return col;
    }

    /** {@inheritDoc} */
    @Override
    public <C extends List<?>> C readList(String name, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = state.item().stream;

        Collection<?> col = stream.readCollection(itemType, this);

        lastRead = stream.lastFinished();

        assert col == null || col instanceof List : col;

        return (C) col;
    }

    /** {@inheritDoc} */
    @Override
    public <C extends Set<?>> C readSet(String name, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = state.item().stream;

        C col = stream.readSet(itemType, this);

        lastRead = stream.lastFinished();

        return col;
    }

    /** {@inheritDoc} */
    @Override
    public <M extends Map<?, ?>> M readMap(String name, MessageCollectionItemType keyType,
            MessageCollectionItemType valType, boolean linked) {
        DirectByteBufferStream stream = state.item().stream;

        M map = stream.readMap(keyType, valType, linked, this);

        lastRead = stream.lastFinished();

        return map;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLastRead() {
        return lastRead;
    }

    /** {@inheritDoc} */
    @Override
    public int state() {
        return state.item().state;
    }

    /** {@inheritDoc} */
    @Override
    public void incrementState() {
        state.item().state++;
    }

    /** {@inheritDoc} */
    @Override
    public void beforeInnerMessageRead() {
        state.forward();
    }

    /** {@inheritDoc} */
    @Override
    public void afterInnerMessageRead(boolean finished) {
        state.backward(finished);
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        state.reset();
    }

    /**
     * Returns a stream to read message fields recursively.
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
        /** Stream. */
        private final DirectByteBufferStream stream;

        /** State. */
        private int state;

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
        }
    }
}
