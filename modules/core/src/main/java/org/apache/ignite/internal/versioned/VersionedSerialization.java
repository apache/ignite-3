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

package org.apache.ignite.internal.versioned;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;

/**
 * The entry point API to work with versioned serialization (used to persist objects).
 *
 * <p>To make it possible to persist an object, a {@link VersionedSerializer} is to be implemented and used with
 * {@link #toBytes(Object, VersionedSerializer)} and {@link #fromBytes(byte[], VersionedSerializer)}.
 *
 * <p>It is responsibility of a serializer to support transparent object structure changes (addition of fields and so on).
 *
 * @see VersionedSerializer
 */
public class VersionedSerialization {
    /** Initial capacity (in bytes) of the buffer used for data output. */
    private static final int INITIAL_BUFFER_CAPACITY = 256;

    /**
     * Converts an object to bytes (including protocol version) that can be later converted back to an object
     * using {@link #fromBytes(byte[], VersionedSerializer)}.
     *
     * @param object Object to serialize.
     * @param serializer Serializer to do the serialization.
     * @return Bytes representing the object.
     * @see #fromBytes(byte[], VersionedSerializer)
     */
    public static <T> byte[] toBytes(T object, VersionedSerializer<T> serializer) {
        try (IgniteUnsafeDataOutput out = new IgniteUnsafeDataOutput(INITIAL_BUFFER_CAPACITY)) {
            serializer.writeExternal(object, out);

            return out.array();
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Cannot serialize", e);
        }
    }

    /**
     * Deserializes an object serialized with {@link #toBytes(Object, VersionedSerializer)}.
     *
     * @param bytes Bytes to deserialize from.
     * @param serializer Serializer to use.
     * @return Deserialized object.
     */
    public static <T> T fromBytes(byte[] bytes, VersionedSerializer<T> serializer) {
        IgniteUnsafeDataInput in = new IgniteUnsafeDataInput(bytes);

        try {
            T result = serializer.readExternal(in);

            if (in.available() != 0) {
                throw new IOException(in.available() + " bytes left unread after deserializing " + result);
            }

            return result;
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Cannot deserialize", e);
        }
    }
}
