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

import java.lang.reflect.Field;

/**
 * Field descriptor for the user object serialization.
 */
public class FieldDescriptor implements DeclaredType {
    /**
     * Name of the field.
     */
    private final String name;

    /**
     * Type of the field (represented by a local class). Local means 'on this machine', but the descriptor could
     * be created on a remote machine where a class with same name could represent a different class.
     */
    private final Class<?> localClass;

    /**
     * Field type's descriptor id.
     */
    private final int typeDescriptorId;

    /**
     * Whether the field is serialized as unshared from the point of view of Java Serialization specification.
     */
    private final boolean unshared;

    private final boolean isPrimitive;
    private final boolean isSerializationTypeKnownUpfront;

    /**
     * Accessor for accessing this field.
     */
    private final FieldAccessor accessor;

    /**
     * Creates a {@link FieldDescriptor} from a local {@link Field}.
     */
    public static FieldDescriptor local(Field field, int typeDescriptorId) {
        return new FieldDescriptor(
                field.getName(),
                field.getType(),
                typeDescriptorId,
                false,
                field.getType().isPrimitive(),
                Classes.isSerializationTypeKnownUpfront(field.getType()),
                FieldAccessor.forField(field)
        );
    }

    /**
     * Creates a {@link FieldDescriptor} from a field defined by name, but defined locally.
     */
    public static FieldDescriptor local(
            String fieldName,
            Class<?> fieldClazz,
            int typeDescriptorId,
            boolean unshared,
            Class<?> declaringClass) {
        return new FieldDescriptor(
                fieldName,
                fieldClazz,
                typeDescriptorId,
                unshared,
                fieldClazz.isPrimitive(),
                Classes.isSerializationTypeKnownUpfront(fieldClazz),
                declaringClass
        );
    }

    /**
     * Creates a {@link FieldDescriptor} for a remote field when the defining class is present locally.
     */
    public static FieldDescriptor remote(
            String fieldName,
            Class<?> fieldClazz,
            int typeDescriptorId,
            boolean unshared,
            boolean isPrimitive,
            boolean isSerializationTypeKnownUpfront,
            Class<?> declaringClass) {
        return new FieldDescriptor(
                fieldName,
                fieldClazz,
                typeDescriptorId,
                unshared,
                isPrimitive,
                isSerializationTypeKnownUpfront,
                declaringClass
        );
    }

    /**
     * Creates a {@link FieldDescriptor} for a remote field when the defining class is not present locally.
     * The resulting FieldDescriptor cannot be used for accessing the field using {@link #accessor()} (because the
     * returned {@link FieldAccessor} will throw on any access attempt).
     */
    public static FieldDescriptor remote(
            String fieldName,
            Class<?> fieldClazz,
            int typeDescriptorId,
            boolean unshared,
            boolean isPrimitive,
            boolean isSerializationTypeKnownUpfront,
            String declaringClassName) {
        return new FieldDescriptor(
                fieldName,
                fieldClazz,
                typeDescriptorId,
                unshared,
                isPrimitive,
                isSerializationTypeKnownUpfront,
                new BrokenFieldAccessor(fieldName, declaringClassName)
        );
    }

    /**
     * Constructor.
     */
    private FieldDescriptor(
            String fieldName,
            Class<?> fieldClazz,
            int typeDescriptorId,
            boolean unshared,
            boolean isPrimitive,
            boolean isSerializationTypeKnownUpfront,
            Class<?> declaringClass) {
        this(
                fieldName,
                fieldClazz,
                typeDescriptorId,
                unshared,
                isPrimitive,
                isSerializationTypeKnownUpfront,
                FieldAccessor.forFieldName(fieldName, declaringClass)
        );
    }

    private FieldDescriptor(
            String fieldName,
            Class<?> fieldClazz,
            int typeDescriptorId,
            boolean unshared,
            boolean isPrimitive,
            boolean isSerializationTypeKnownUpfront,
            FieldAccessor accessor
    ) {
        this.name = fieldName;
        this.localClass = fieldClazz;
        this.typeDescriptorId = typeDescriptorId;
        this.unshared = unshared;
        this.isPrimitive = isPrimitive;
        this.isSerializationTypeKnownUpfront = isSerializationTypeKnownUpfront;
        this.accessor = accessor;
    }

    /**
     * Returns field's name.
     *
     * @return Field's name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns type of the field (represented by a local class). Local means 'on this machine', but the descriptor could
     * be created on a remote machine where a class with same name could represent a different class..
     *
     * @return Field's type (represented by a local class).
     */
    public Class<?> localClass() {
        return localClass;
    }

    /**
     * Returns the field type name.
     *
     * @return field type name
     */
    public String typeName() {
        return localClass.getName();
    }

    /** {@inheritDoc} */
    @Override
    public int typeDescriptorId() {
        return typeDescriptorId;
    }

    /**
     * Returns whether the field is serialized as unshared from the point of view of Java Serialization specification.
     *
     * @return whether the field is serialized as unshared from the point of view of Java Serialization specification
     */
    public boolean isUnshared() {
        return unshared;
    }

    /**
     * Returns {@code true} if this field has a primitive type.
     *
     * @return {@code true} if this field has a primitive type
     */
    public boolean isPrimitive() {
        return isPrimitive;
    }

    /**
     * Returns number of bytes representing this primitive field type.
     *
     * @return number of bytes representing this primitive field type
     */
    public int primitiveWidthInBytes() {
        assert isPrimitive();

        return Primitives.widthInBytes(localClass);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSerializationTypeKnownUpfront() {
        return isSerializationTypeKnownUpfront;
    }

    /**
     * Returns {@link FieldAccessor} for this field.
     *
     * @return {@link FieldAccessor} for this field
     */
    public FieldAccessor accessor() {
        return accessor;
    }
}
