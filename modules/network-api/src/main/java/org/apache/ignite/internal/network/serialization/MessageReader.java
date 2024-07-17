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
import java.util.LinkedHashMap;
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
 * Stateful message reader.
 */
public interface MessageReader {
    /**
     * Sets the byte buffer to read from.
     *
     * @param buf Byte buffer.
     */
    void setBuffer(ByteBuffer buf);

    /**
     * Sets the type of the message that is currently being read.
     *
     * @param msgCls Message type.
     */
    void setCurrentReadClass(Class<? extends NetworkMessage> msgCls);

    /**
     * Callback that must be invoked by implementations of message serializers before they start decoding the message body.
     *
     * @return {@code True} if a read operation is allowed to proceed, {@code false} otherwise.
     */
    boolean beforeMessageRead();

    /**
     * Callback that must be invoked by implementations of message serializers after they finished decoding the message body.
     *
     * @param msgCls Class of the message that is finishing read stage.
     * @return {@code True} if a read operation can be proceeded, {@code false} otherwise.
     */
    boolean afterMessageRead(Class<? extends NetworkMessage> msgCls);

    /**
     * Reads a {@code byte} value.
     *
     * @param name Field name.
     * @return {@code byte} value.
     */
    byte readByte(String name);

    /**
     * Reads a {@code Byte} value.
     *
     * @param name Field name.
     * @return {@code Byte} value.
     */
    @Nullable Byte readBoxedByte(String name);

    /**
     * Reads a {@code short} value.
     *
     * @param name Field name.
     * @return {@code short} value.
     */
    short readShort(String name);

    /**
     * Reads a {@code Short} value.
     *
     * @param name Field name.
     * @return {@code Short} value.
     */
    @Nullable Short readBoxedShort(String name);

    /**
     * Reads an {@code int} value.
     *
     * @param name Field name.
     * @return {@code int} value.
     */
    int readInt(String name);

    /**
     * Reads an {@code int} value.
     *
     * @param name Field name.
     * @param dflt A default value if the field is not found.
     * @return {@code int} value.
     */
    int readInt(String name, int dflt);

    /**
     * Reads an {@code Integer} value.
     *
     * @param name Field name.
     * @return {@code Integer} value.
     */
    @Nullable Integer readBoxedInt(String name);

    /**
     * Reads a {@code long} value.
     *
     * @param name Field name.
     * @return {@code long} value.
     */
    long readLong(String name);

    /**
     * Reads a {@code Long} value.
     *
     * @param name Field name.
     * @return {@code Long} value.
     */
    @Nullable Long readBoxedLong(String name);

    /**
     * Reads a {@code float} value.
     *
     * @param name Field name.
     * @return {@code float} value.
     */
    float readFloat(String name);

    /**
     * Reads a {@code Float} value.
     *
     * @param name Field name.
     * @return {@code Float} value.
     */
    @Nullable Float readBoxedFloat(String name);

    /**
     * Reads a {@code double} value.
     *
     * @param name Field name.
     * @return {@code double} value.
     */
    double readDouble(String name);

    /**
     * Reads a {@code Double} value.
     *
     * @param name Field name.
     * @return {@code Double} value.
     */
    @Nullable Double readBoxedDouble(String name);

    /**
     * Reads a {@code char} value.
     *
     * @param name Field name.
     * @return {@code char} value.
     */
    char readChar(String name);

    /**
     * Reads a {@code Character} value.
     *
     * @param name Field name.
     * @return {@code Character} value.
     */
    @Nullable Character readBoxedChar(String name);

    /**
     * Reads a {@code boolean} value.
     *
     * @param name Field name.
     * @return {@code boolean} value.
     */
    boolean readBoolean(String name);

    /**
     * Reads a {@code Boolean} value.
     *
     * @param name Field name.
     * @return {@code Boolean} value.
     */
    @Nullable Boolean readBoxedBoolean(String name);

    /**
     * Reads a {@code byte} array.
     *
     * @param name Field name.
     * @return {@code byte} array.
     */
    byte[] readByteArray(String name);

    /**
     * Reads a {@code short} array.
     *
     * @param name Field name.
     * @return {@code short} array.
     */
    short[] readShortArray(String name);

    /**
     * Reads an {@code int} array.
     *
     * @param name Field name.
     * @return {@code int} array.
     */
    int[] readIntArray(String name);

    /**
     * Reads a {@code long} array.
     *
     * @param name Field name.
     * @return {@code long} array.
     */
    long[] readLongArray(String name);

    /**
     * Reads a {@code float} array.
     *
     * @param name Field name.
     * @return {@code float} array.
     */
    float[] readFloatArray(String name);

    /**
     * Reads a {@code double} array.
     *
     * @param name Field name.
     * @return {@code double} array.
     */
    double[] readDoubleArray(String name);

    /**
     * Reads a {@code char} array.
     *
     * @param name Field name.
     * @return {@code char} array.
     */
    char[] readCharArray(String name);

    /**
     * Reads a {@code boolean} array.
     *
     * @param name Field name.
     * @return {@code boolean} array.
     */
    boolean[] readBooleanArray(String name);

    /**
     * Reads a {@link String}.
     *
     * @param name Field name.
     * @return {@link String}.
     */
    String readString(String name);

    /**
     * Reads a {@link BitSet}.
     *
     * @param name Field name.
     * @return {@link BitSet}.
     */
    BitSet readBitSet(String name);

    /**
     * Reads a {@link ByteBuffer}.
     *
     * @param name Field name.
     * @return {@link ByteBuffer}.
     */
    ByteBuffer readByteBuffer(String name);

    /**
     * Reads an {@link UUID}.
     *
     * @param name Field name.
     * @return {@link UUID}.
     */
    UUID readUuid(String name);

    /**
     * Reads an {@link IgniteUuid}.
     *
     * @param name Field name.
     * @return {@link IgniteUuid}.
     */
    IgniteUuid readIgniteUuid(String name);

    /**
     * Reads an {@link HybridTimestamp}.
     *
     * @param name Field name.
     */
    @Nullable HybridTimestamp readHybridTimestamp(String name);

    /**
     * Reads a group type or a message type of the message. Unlike regular {@link #readShort(String)}, this method never accepts a field
     * name, because there are no names assigned to the header values.
     *
     * @return {@code short} value.
     */
    short readHeaderShort();

    /**
     * Reads a nested message.
     *
     * @param <T> Type of a message;
     * @param name Field name.
     * @return Message.
     */
    <T extends NetworkMessage> T readMessage(String name);

    /**
     * Reads an array of objects.
     *
     * @param <T> Type of an array.
     * @param name Field name.
     * @param itemType A component type of the array.
     * @param itemCls A component class of the array.
     * @return Array of objects.
     */
    <T> T[] readObjectArray(String name, MessageCollectionItemType itemType, Class<T> itemCls);

    /**
     * Reads a collection.
     *
     * @param <C> Type of collection.
     * @param name Field name.
     * @param itemType An item type of the collection.
     * @return Collection.
     */
    <C extends Collection<?>> C readCollection(String name, MessageCollectionItemType itemType);

    /**
     * Reads a list.
     *
     * @param <C> Type of list.
     * @param name Field name.
     * @param itemType An item type of the list.
     * @return List.
     */
    <C extends List<?>> C readList(String name, MessageCollectionItemType itemType);

    /**
     * Reads a set.
     *
     * @param <C> Type of set.
     * @param name Field name.
     * @param itemType An item type of the set.
     * @return Set.
     */
    <C extends Set<?>> C readSet(String name, MessageCollectionItemType itemType);

    /**
     * Reads a map.
     *
     * @param <M> Type of a map.
     * @param name Field name.
     * @param keyType The type of the map's key.
     * @param valType The type of the map's value.
     * @param linked Whether a {@link LinkedHashMap} should be created.
     * @return Map.
     */
    <M extends Map<?, ?>> M readMap(String name, MessageCollectionItemType keyType,
            MessageCollectionItemType valType, boolean linked);

    /**
     * Tells whether the last invocation of any of the {@code readXXX(...)} methods has fully written the value. {@code False} is returned
     * if there were not enough remaining bytes in a byte buffer.
     *
     * @return Whether the last value was fully read.
     */
    boolean isLastRead();

    /**
     * Gets a current read state.
     *
     * @return Read state.
     */
    int state();

    /**
     * Increments a read state.
     */
    void incrementState();

    /**
     * Callback called before an inner message is read.
     */
    void beforeInnerMessageRead();

    /**
     * Callback called after an inner message is read.
     *
     * @param finished Whether a message was fully read.
     */
    void afterInnerMessageRead(boolean finished);

    /**
     * Resets this reader.
     */
    void reset();
}
