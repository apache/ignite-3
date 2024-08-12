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

package org.apache.ignite.marshalling;


import org.jetbrains.annotations.Nullable;

/**
 * Object marshaller interface that is used in every Ignite API that requires serialization/deserialization of user objects. If you want to
 * define the way your objects are serialized/deserialized, you can implement this interface and pass it to the API that requires it.
 *
 * <p>NOTE: The marshaller itself are not sent over the wire. This means that if you
 * define a custom marshaller on the client, you must also define the marshaller on the server as well.
 *
 * @param <T> The object (T)ype.
 * @param <R> The (R)aw type, for example, {@code byte[]}.
 */
public interface Marshaller<T, R> {
    /**
     * Marshal the object into raw type.
     *
     * @param object object to marshal.
     * @return raw presentation of object.
     * @throws UnsupportedObjectTypeMarshallingException if the given type can not be marshalled with current instance.
     */
    @Nullable
    R marshal(@Nullable T object) throws UnsupportedObjectTypeMarshallingException;

    /**
     * Unmarshal the raw type into object.
     *
     * @param raw raw presentation of object.
     * @return object.
     * @throws UnsupportedObjectTypeMarshallingException if the given type can not be unmarshalled with current instance.
     */
    @Nullable
    T unmarshal(@Nullable R raw) throws UnsupportedObjectTypeMarshallingException;

    /**
     * Try to marshal given object if marshaller if not null, else the object is casted directly to the target type.
     *
     * @param self the marshaller instance.
     * @param object to marshal.
     * @param <T> The object (T)ype.
     * @param <R> The (R)aw type, for example, {@code byte[]} or {@link org.apache.ignite.table.Tuple}.
     */
    static <T, R> @Nullable R tryMarshalOrCast(@Nullable Marshaller<T, R> self, @Nullable Object object) {
        if (self != null) {
            try {
                T castedObj = (T) object;
                return self.marshal(castedObj);
            } catch (ClassCastException e) {
                // todo: log.
            }
        }

        return (R) object;
    }

    /**
     * Try to unmarshal given object if marshaller if not null, else the object is casted directly to the target type.
     *
     * @param self the marshaller instance.
     * @param <T> The object (T)ype.
     * @param <R> The (R)aw type, for example, {@code byte[]}.
     */
    static <T, R> @Nullable T tryUnmarshalOrCast(@Nullable Marshaller<T, R> self, @Nullable Object raw) {
        if (self != null) {
            try {
                R rawType = (R) raw;
                return self.unmarshal(rawType);
            } catch (ClassCastException e) {
                // todo: log.
            }
        }

        return (T) raw;
    }
}

