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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.jetbrains.annotations.Nullable;

/**
 * Default java serialization marshaller. It is used by default if no other marshaller is provided.
 */
class JavaSerializationByteArrayMarshallilng {
    /**
     * Writes the object to a byte array with java serialization.
     *
     * @param <T> object.
     * @return byte array that represents the object.
     */
    public static <T extends Serializable> byte @Nullable [] marshal(T object) {
        if (object == null) {
            return null;
        }

        try (var baos = new ByteArrayOutputStream(); var out = new ObjectOutputStream(baos)) {
            out.writeObject(object);
            out.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            throw new MarshallingException(e);
        }
    }

    /**
     * Reads the object from a byte array with java serialization.
     *
     * @param raw byte array that represents the object.
     * @return object.
     */
    public static <T> @Nullable T unmarshal(byte @Nullable [] raw) {
        if (raw == null) {
            return null;
        }

        try (var bais = new ByteArrayInputStream(raw); var ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new MarshallingException(e);
        }
    }
}
