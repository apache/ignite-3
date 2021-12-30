/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.lang.reflect.Modifier;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Class descriptor for the user object serialization.
 */
public class ClassDescriptor {
    /**
     * Name of the class.
     */
    @NotNull
    private final String className;

    /**
     * Class.
     */
    @NotNull
    private final Class<?> clazz;

    /**
     * Descriptor id.
     */
    private final int descriptorId;

    /**
     * List of the class fields' descriptors.
     */
    @NotNull
    private final List<FieldDescriptor> fields;

    /**
     * How the class is to be serialized.
     */
    private final Serialization serialization;

    /**
     * Whether the class is final.
     */
    private final boolean isFinal;

    private final SpecialSerializationMethods serializationMethods;

    /**
     * Constructor.
     */
    public ClassDescriptor(@NotNull Class<?> clazz, int descriptorId, @NotNull List<FieldDescriptor> fields, Serialization serialization) {
        this.className = clazz.getName();
        this.clazz = clazz;
        this.descriptorId = descriptorId;
        this.fields = List.copyOf(fields);
        this.serialization = serialization;
        this.isFinal = Modifier.isFinal(clazz.getModifiers());

        serializationMethods = new SpecialSerializationMethodsImpl(this);
    }

    /**
     * Returns descriptor id.
     *
     * @return Descriptor id.
     */
    public int descriptorId() {
        return descriptorId;
    }

    /**
     * Returns fields' descriptors.
     *
     * @return Fields' descriptors.
     */
    @NotNull
    public List<FieldDescriptor> fields() {
        return fields;
    }

    /**
     * Returns class' name.
     *
     * @return Class' name.
     */
    @NotNull
    public String className() {
        return className;
    }

    /**
     * Returns descriptor's class.
     *
     * @return Class.
     */
    @NotNull
    public Class<?> clazz() {
        return clazz;
    }

    /**
     * Returns serialization.
     *
     * @return Serialization.
     */
    public Serialization serialization() {
        return serialization;
    }

    /**
     * Returns serialization type.
     *
     * @return Serialization type.
     */
    public SerializationType serializationType() {
        return serialization.type();
    }

    /**
     * Returns {@code true} if class is final, {@code false} otherwise.
     *
     * @return {@code true} if class is final, {@code false} otherwise.
     */
    public boolean isFinal() {
        return isFinal;
    }

    /**
     * Returns {@code true} if the described class should be serialized as a {@link java.io.Serializable} (but not
     * using the mechanism for {@link java.io.Externalizable}).
     *
     * @return {@code true} if the described class should be serialized as a {@link java.io.Serializable}.
     */
    public boolean isSerializable() {
        return serialization.type() == SerializationType.SERIALIZABLE;
    }

    /**
     * Returns {@code true} if the described class should be serialized as an {@link java.io.Externalizable}.
     *
     * @return {@code true} if the described class should be serialized as an {@link java.io.Externalizable}.
     */
    public boolean isExternalizable() {
        return serialization.type() == SerializationType.EXTERNALIZABLE;
    }

    /**
     * Returns {@code true} if the described class is treated as a built-in.
     *
     * @return {@code true} if if the described class is treated as a built-in
     */
    public boolean isBuiltIn() {
        return serializationType() == SerializationType.BUILTIN;
    }

    /**
     * Returns {@code true} if the described class has {@code writeReplace()} method.
     *
     * @return {@code true} if the described class has {@code writeReplace()} method
     */
    public boolean hasWriteReplace() {
        return serialization.hasWriteReplace();
    }

    /**
     * Returns {@code true} if the described class has {@code readResolve()} method.
     *
     * @return {@code true} if the described class has {@code readResolve()} method
     */
    public boolean hasReadResolve() {
        return serialization.hasReadResolve();
    }

    /**
     * Returns {@code true} if this is the descriptor of {@code null} values.
     *
     * @return {@code true} if this is the descriptor of {@code null} values
     */
    public boolean isNull() {
        return descriptorId == BuiltinType.NULL.descriptorId();
    }

    /**
     * Returns {@code true} if this is the descriptor of {@link java.util.Collections#singletonList(Object)} type.
     *
     * @return {@code true} if this is the descriptor of {@link java.util.Collections#singletonList(Object)} type
     */
    public boolean isSingletonList() {
        return descriptorId == BuiltinType.SINGLETON_LIST.descriptorId();
    }

    /**
     * Returns {@code true} if the described class has writeReplace() method, and it makes sense for the needs of
     * our serialization (i.e. it is SERIALIZABLE or EXTERNALIZABLE).
     *
     * @return {@code true} if the described class has writeReplace() method, and it makes sense for the needs of
     *     our serialization
     */
    public boolean supportsWriteReplace() {
        return (isSerializable() || isExternalizable()) && hasWriteReplace();
    }

    /**
     * Returns special serialization methods facility.
     *
     * @return special serialization methods facility
     */
    public SpecialSerializationMethods serializationMethods() {
        return serializationMethods;
    }

    @Override
    public String toString() {
        return "ClassDescriptor{"
                + "className='" + className + '\''
                + ", descriptorId=" + descriptorId
                + '}';
    }
}
