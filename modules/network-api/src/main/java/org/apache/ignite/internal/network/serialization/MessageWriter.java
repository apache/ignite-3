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

package org.apache.ignite.internal.network.serialization;

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
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.jetbrains.annotations.Nullable;

/**
 * Stateful message writer.
 */
public interface MessageWriter {
    /**
     * Sets the byte buffer to write to.
     *
     * @param buf Byte buffer.
     */
    void setBuffer(ByteBuffer buf);

    /**
     * Sets the type of the message that is currently being written.
     *
     * @param msgCls Message type.
     */
    void setCurrentWriteClass(Class<? extends NetworkMessage> msgCls);

    /**
     * Writes the header of a message.
     *
     * @param groupType Message group type.
     * @param messageType Message type.
     * @param fieldCnt Fields count.
     * @return {@code true} if successfully. Otherwise returns {@code false}.
     */
    boolean writeHeader(short groupType, short messageType, byte fieldCnt);

    /**
     * Writes a {@code byte} value.
     *
     * @param name Field name.
     * @param val {@code byte} value.
     * @return Whether a value was fully written.
     */
    boolean writeByte(String name, byte val);

    /**
     * Writes a {@code Byte} value.
     *
     * @param name Field name.
     * @param val {@code Byte} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedByte(String name, @Nullable Byte val);

    /**
     * Writes a {@code short} value.
     *
     * @param name Field name.
     * @param val {@code short} value.
     * @return Whether a value was fully written.
     */
    boolean writeShort(String name, short val);

    /**
     * Writes a {@code Short} value.
     *
     * @param name Field name.
     * @param val {@code Short} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedShort(String name, @Nullable Short val);

    /**
     * Writes an {@code int} value.
     *
     * @param name Field name.
     * @param val {@code int} value.
     * @return Whether a value was fully written.
     */
    boolean writeInt(String name, int val);

    /**
     * Writes an {@code Integer} value.
     *
     * @param name Field name.
     * @param val {@code Integer} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedInt(String name, @Nullable Integer val);

    /**
     * Writes a {@code long} value.
     *
     * @param name Field name.
     * @param val {@code long} value.
     * @return Whether a value was fully written.
     */
    boolean writeLong(String name, long val);

    /**
     * Writes a {@code Long} value.
     *
     * @param name Field name.
     * @param val {@code Long} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedLong(String name, @Nullable Long val);

    /**
     * Writes a {@code float} value.
     *
     * @param name Field name.
     * @param val {@code float} value.
     * @return Whether a value was fully written.
     */
    boolean writeFloat(String name, float val);

    /**
     * Writes a {@code Float} value.
     *
     * @param name Field name.
     * @param val {@code Float} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedFloat(String name, @Nullable Float val);

    /**
     * Writes a {@code double} value.
     *
     * @param name Field name.
     * @param val {@code double} value.
     * @return Whether a value was fully written.
     */
    boolean writeDouble(String name, double val);

    /**
     * Writes a {@code Double} value.
     *
     * @param name Field name.
     * @param val {@code Short} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedDouble(String name, @Nullable Double val);

    /**
     * Writes a {@code char} value.
     *
     * @param name Field name.
     * @param val {@code char} value.
     * @return Whether a value was fully written.
     */
    boolean writeChar(String name, char val);

    /**
     * Writes a {@code Character} value.
     *
     * @param name Field name.
     * @param val {@code Character} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedChar(String name, @Nullable Character val);

    /**
     * Writes a {@code boolean} value.
     *
     * @param name Field name.
     * @param val {@code boolean} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoolean(String name, boolean val);

    /**
     * Writes a {@code Boolean} value.
     *
     * @param name Field name.
     * @param val {@code Boolean} value.
     * @return Whether a value was fully written.
     */
    boolean writeBoxedBoolean(String name, @Nullable Boolean val);

    /**
     * Writes a {@code byte} array.
     *
     * @param name Field name.
     * @param val {@code byte} array.
     * @return Whether an array was fully written.
     */
    boolean writeByteArray(String name, byte[] val);

    /**
     * Writes a {@code byte} array.
     *
     * @param name Field name.
     * @param val {@code byte} array.
     * @param off Offset.
     * @param len Length.
     * @return Whether an array was fully written.
     */
    boolean writeByteArray(String name, byte[] val, long off, int len);

    /**
     * Writes a {@code short} array.
     *
     * @param name Field name.
     * @param val {@code short} array.
     * @return Whether an array was fully written.
     */
    boolean writeShortArray(String name, short[] val);

    /**
     * Writes an {@code int} array.
     *
     * @param name Field name.
     * @param val {@code int} array.
     * @return Whether an array was fully written.
     */
    boolean writeIntArray(String name, int[] val);

    /**
     * Writes a {@code long} array.
     *
     * @param name Field name.
     * @param val {@code long} array.
     * @return Whether an array was fully written.
     */
    boolean writeLongArray(String name, long[] val);

    /**
     * Writes a {@code long} array.
     *
     * @param name Field name.
     * @param val {@code long} array.
     * @param len Length.
     * @return Whether an array was fully written.
     */
    boolean writeLongArray(String name, long[] val, int len);

    /**
     * Writes a {@code float} array.
     *
     * @param name Field name.
     * @param val {@code float} array.
     * @return Whether an array was fully written.
     */
    boolean writeFloatArray(String name, float[] val);

    /**
     * Writes a {@code double} array.
     *
     * @param name Field name.
     * @param val {@code double} array.
     * @return Whether an array was fully written.
     */
    boolean writeDoubleArray(String name, double[] val);

    /**
     * Writes a {@code char} array.
     *
     * @param name Field name.
     * @param val {@code char} array.
     * @return Whether an array was fully written.
     */
    boolean writeCharArray(String name, char[] val);

    /**
     * Writes a {@code boolean} array.
     *
     * @param name Field name.
     * @param val {@code boolean} array.
     * @return Whether an array was fully written.
     */
    boolean writeBooleanArray(String name, boolean[] val);

    /**
     * Writes a {@link String}.
     *
     * @param name Field name.
     * @param val {@link String}.
     * @return Whether a value was fully written.
     */
    boolean writeString(String name, String val);

    /**
     * Writes a {@link BitSet}.
     *
     * @param name Field name.
     * @param val {@link BitSet}.
     * @return Whether a value was fully written.
     */
    boolean writeBitSet(String name, BitSet val);

    /**
     * Writes a {@link ByteBuffer}.
     *
     * @param name Field name.
     * @param val {@link ByteBuffer}.
     * @return Whether a value was fully written.
     */
    boolean writeByteBuffer(String name, ByteBuffer val);

    /**
     * Writes an {@link UUID}.
     *
     * @param name Field name.
     * @param val {@link UUID}.
     * @return Whether a value was fully written.
     */
    boolean writeUuid(String name, UUID val);

    /**
     * Writes an {@link IgniteUuid}.
     *
     * @param name Field name.
     * @param val {@link IgniteUuid}.
     * @return Whether a value was fully written.
     */
    boolean writeIgniteUuid(String name, IgniteUuid val);

    /**
     * Writes an {@link HybridTimestamp}.
     *
     * @param name Field name.
     * @param val {@link HybridTimestamp}.
     * @return Whether a value was fully written.
     */
    boolean writeHybridTimestamp(String name, @Nullable HybridTimestamp val);

    /**
     * Writes a nested message.
     *
     * @param name Field name.
     * @param val Message.
     * @return Whether a value was fully written.
     */
    boolean writeMessage(String name, NetworkMessage val);

    /**
     * Writes an array of objects.
     *
     * @param <T> Type of an array.
     * @param name Field name.
     * @param arr Array of objects.
     * @param itemType A component type of the array.
     * @return Whether an array was fully written.
     */
    <T> boolean writeObjectArray(String name, T[] arr, MessageCollectionItemType itemType);

    /**
     * Writes collection.
     *
     * @param <T> Type of a collection.
     * @param name Field name.
     * @param col Collection.
     * @param itemType An item type of the collection.
     * @return Whether a value was fully written.
     */
    <T> boolean writeCollection(String name, Collection<T> col, MessageCollectionItemType itemType);

    /**
     * Writes a list.
     *
     * @param <T> Type of list.
     * @param name Field name.
     * @param col Collection.
     * @param itemType An item type of the list.
     * @return Whether a value was fully written.
     */
    <T> boolean writeList(String name, List<T> col, MessageCollectionItemType itemType);

    /**
     * Writes a set.
     *
     * @param <T> Type of set.
     * @param name Field name.
     * @param col Collection.
     * @param itemType An item type of the set.
     * @return Whether a value was fully written.
     */
    <T> boolean writeSet(String name, Set<T> col, MessageCollectionItemType itemType);

    /**
     * Writes a map.
     *
     * @param <K> Type of the map's keys.
     * @param <V> Type of the map's values.
     * @param name Field name.
     * @param map Map.
     * @param keyType The type of the map's key.
     * @param valType The type of the map's value.
     * @return Whether a value was fully written.
     */
    <K, V> boolean writeMap(String name, Map<K, V> map, MessageCollectionItemType keyType,
            MessageCollectionItemType valType);

    /**
     * Returns {@code true} if the header of the current message has been written, {@code false} otherwise.
     *
     * @return Whether the message header has already been written.
     */
    boolean isHeaderWritten();

    /**
     * Callback called when the header of the message is written.
     */
    void onHeaderWritten();

    /**
     * Gets a current message state.
     *
     * @return State.
     */
    int state();

    /**
     * Increments a state.
     */
    void incrementState();

    /**
     * Callback called before an inner message is written.
     */
    void beforeInnerMessageWrite();

    /**
     * Callback called after an inner message is written.
     *
     * @param finished Whether the message was fully written.
     */
    void afterInnerMessageWrite(boolean finished);

    /**
     * Resets this writer.
     */
    void reset();
}
