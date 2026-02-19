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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import org.jetbrains.annotations.Nullable;

/**
 * The ByteArrayMarshaller interface is designed to marshal and unmarshal objects to and from byte arrays. If the byte[] is your preferred
 * way of marshalling objects, you can implement this interface to provide custom marshalling logic.
 *
 * <p>The default implementation of the marshal method is plain java serialization.
 */
public interface ByteArrayMarshaller<T> extends Marshaller<T, byte[]> {

    static <T> ByteArrayMarshaller<T> create() {
        return new ByteArrayMarshaller<>() {};
    }

    @Override
    default byte @Nullable [] marshal(@Nullable T object) {
        if (object == null) {
            return null;
        }

        if (object instanceof Serializable) {
            try (var baos = new ByteArrayOutputStream(); var out = new ObjectOutputStream(baos)) {
                out.writeObject(object);
                out.flush();

                return baos.toByteArray();
            } catch (IOException e) {
                throw new MarshallingException(e);
            }
        }

        String msg = object.getClass() + " must be Serializable to be used with ByteArrayMarshaller.";
        throw new UnsupportedObjectTypeMarshallingException(msg);
    }

    @Override
    default @Nullable T unmarshal(byte @Nullable [] raw) {
        if (raw == null) {
            return null;
        }

        try (var bais = new ByteArrayInputStream(raw); var ois = new ObjectInputStream(bais) {
            /*
             * Why do we subclass ObjectInputStream here?
             *
             * - Class loading in a distributed / runtime‑extensible environment:
             *   Ignite often needs to deserialize user classes that are not visible to the
             *   system/application class loader (for example, user code deployed as a compute unit,
             *   classes loaded by a job/unit-specific ClassLoader).
             *   The default ObjectInputStream#resolveClass uses an internal heuristic ("latest
             *   user-defined loader") that frequently ends up being the system or context class loader.
             *   That loader may not see such user classes, leading to ClassNotFoundException during
             *   deserialization.
             *
             * - Deterministic loader selection:
             *   We explicitly resolve classes using the ClassLoader set in the thread's context. In Ignite, this loader is set up before
             *   the unmarshal call to the job/unit ClassLoader. This makes class resolution deterministic and aligned with
             *   the deployment context, mirroring the environment where serialization occurred.
             */
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                String name = desc.getName();
                try {
                    return Class.forName(name, false, Thread.currentThread().getContextClassLoader());
                } catch (ClassNotFoundException ex) {
                    return super.resolveClass(desc);
                }
            }
        }) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new MarshallingException(e);
        }
    }
}
