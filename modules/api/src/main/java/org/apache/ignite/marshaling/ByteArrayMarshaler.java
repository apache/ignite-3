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

package org.apache.ignite.marshaling;

import java.io.Serializable;
import org.jetbrains.annotations.Nullable;

/**
 * The ByteArrayMarshaler interface is designed to marshal and unmarshal objects to and from byte arrays. If the byte[] is your preferred
 * way of marshaling objects, you can implement this interface to provide custom marshaling logic.
 *
 * <p>The default implementation of the marshal method is plain java serialization.
 */
public interface ByteArrayMarshaler<T> extends Marshaler<T, byte[]> {

    static <T> ByteArrayMarshaler<T> create() {
        return new ByteArrayMarshaler<>() {};
    }

    @Override
    default byte @Nullable [] marshal(@Nullable T object) {
        if (object == null) {
            return null;
        }

        if (object instanceof Serializable) {
            return JavaSerializationByteArrayMarshallilng.marshal((Serializable) object);
        }

        throw new UnsupportedObjectTypeMarshalingException(object.getClass());
    }

    @Override
    default @Nullable T unmarshal(byte @Nullable [] raw) {
        if (raw == null) {
            return null;
        }

        return JavaSerializationByteArrayMarshallilng.unmarshal(raw);
    }
}
