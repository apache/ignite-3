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

package org.apache.ignite.internal.network.message;

import java.util.Collection;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.SerializationType;
import org.jetbrains.annotations.Nullable;

/** Message for the {@link ClassDescriptor}. */
@Transferable(NetworkMessageTypes.CLASS_DESCRIPTOR_MESSAGE)
public interface ClassDescriptorMessage extends NetworkMessage {
    int IS_PRIMITIVE_MASK = 1;
    int IS_ARRAY_MASK = 1 << 1;
    int IS_RUNTIME_ENUM_MASK = 1 << 2;
    int IS_SERIALIZATION_TYPE_KNOWN_UPFRONT_MASK = 1 << 3;

    int HAS_WRITE_OBJECT_MASK = 1;
    int HAS_READ_OBJECT_MASK = 1 << 1;
    int HAS_READ_OBJECT_NO_DATA_MASK = 1 << 2;
    int HAS_WRITE_REPLACE_MASK = 1 << 3;
    int HAS_READ_RESOLVE_MASK = 1 << 4;

    /**
     * Class name.
     *
     * @see ClassDescriptor#className()
     */
    String className();

    /**
     * Descriptor id.
     *
     * @see ClassDescriptor#descriptorId()
     */
    int descriptorId();

    /**
     * Super-class name.
     *
     * @see ClassDescriptor#superClassDescriptor()
     */
    @Nullable
    String superClassName();

    /**
     * Super-class descriptor ID. {@code -1} if super-class is missing.
     *
     * @see ClassDescriptor#superClassDescriptor()
     */
    int superClassDescriptorId();

    /**
     * Component type name.
     *
     * @see ClassDescriptor#componentTypeName()
     */
    @Nullable
    String componentTypeName();

    /**
     * Component type descriptor ID. {@code -1} if the described class is not an array class.
     *
     * @see ClassDescriptor#componentTypeDescriptorId()
     */
    int componentTypeDescriptorId();

    /**
     * Returns attribute flags.
     *
     * @return attribute flags
     */
    byte attributes();

    /**
     * List of the class fields' descriptors.
     *
     * @see ClassDescriptor#fields()
     */
    Collection<FieldDescriptorMessage> fields();

    /**
     * The type of the serialization mechanism for the class.
     *
     * @see SerializationType#value()
     */
    byte serializationType();

    /**
     * Serialization-related flags (hasReadObject, hasWriteReplace and so on).
     *
     * @return serialization flags
     */
    byte serializationFlags();
}
