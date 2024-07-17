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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import org.apache.ignite.internal.lang.IgniteUuid;

/**
 * Built-in types.
 */
public enum BuiltInType {
    BYTE(BuiltInTypeIds.BYTE, byte.class),
    BYTE_BOXED(1, Byte.class),
    SHORT(BuiltInTypeIds.SHORT, short.class),
    SHORT_BOXED(3, Short.class),
    INT(BuiltInTypeIds.INT, int.class),
    INT_BOXED(5, Integer.class),
    FLOAT(BuiltInTypeIds.FLOAT, float.class),
    FLOAT_BOXED(7, Float.class),
    LONG(BuiltInTypeIds.LONG, long.class),
    LONG_BOXED(9, Long.class),
    DOUBLE(BuiltInTypeIds.DOUBLE, double.class),
    DOUBLE_BOXED(11, Double.class),
    BOOLEAN(BuiltInTypeIds.BOOLEAN, boolean.class),
    BOOLEAN_BOXED(13, Boolean.class),
    CHAR(14, char.class),
    CHAR_BOXED(15, Character.class),
    /** An object whose class is exactly Object (not a subclass). It is created using new Object(). */
    BARE_OBJECT(16, Object.class),
    STRING(17, String.class),
    UUID(18, java.util.UUID.class),
    IGNITE_UUID(19, IgniteUuid.class),
    DATE(20, Date.class),
    BYTE_ARRAY(21, byte[].class),
    SHORT_ARRAY(22, short[].class),
    INT_ARRAY(23, int[].class),
    FLOAT_ARRAY(24, float[].class),
    LONG_ARRAY(25, long[].class),
    DOUBLE_ARRAY(26, double[].class),
    BOOLEAN_ARRAY(27, boolean[].class),
    CHAR_ARRAY(28, char[].class),
    DECIMAL(29, BigDecimal.class),
    ARRAY_LIST(30, ArrayList.class),
    LINKED_LIST(31, LinkedList.class),
    HASH_SET(32, HashSet.class),
    LINKED_HASH_SET(33, LinkedHashSet.class),
    SINGLETON_LIST(34, Collections.singletonList(null).getClass()),
    HASH_MAP(35, HashMap.class),
    LINKED_HASH_MAP(36, LinkedHashMap.class),
    BIT_SET(37, BitSet.class),
    NULL(38, Null.class),
    REFERENCE(39, ReferencePlaceholder.class),
    CLASS(40, Class.class),
    PROXY(41, Proxy.class),
    STRING_LATIN1(42, StringLatin1Placeholder.class)
    ;

    /**
     * Pre-defined descriptor id.
     */
    private final int descriptorId;

    /**
     * Type.
     */
    private final Class<?> clazz;

    private static final Int2ObjectMap<BuiltInType> descriptorIdToValues;

    static {
        Int2ObjectMap<BuiltInType> map = new Int2ObjectOpenHashMap<>();
        for (BuiltInType type : BuiltInType.values()) {
            map.put(type.descriptorId, type);
        }
        descriptorIdToValues = Int2ObjectMaps.unmodifiable(map);
    }

    private static final Map<Class<?>, BuiltInType> classToValues;

    static {
        Map<Class<?>, BuiltInType> map = new HashMap<>();
        for (BuiltInType type : BuiltInType.values()) {
            map.put(type.clazz, type);
        }
        classToValues = Map.copyOf(map);
    }

    /**
     * Constructor.
     *
     * @param descriptorId Descriptor id.
     * @param clazz        Type.
     */
    BuiltInType(int descriptorId, Class<?> clazz) {
        this.descriptorId = descriptorId;
        this.clazz = clazz;
    }

    /**
     * Finds a built-in type by a descriptor ID.
     *
     * @param descriptorId ID of the corresponding descriptor
     * @return built-in type
     * @throws IllegalArgumentException if no built-in type has the specified descriptor ID
     */
    public static BuiltInType findByDescriptorId(int descriptorId) {
        BuiltInType type = descriptorIdToValues.get(descriptorId);
        if (type == null) {
            throw new IllegalArgumentException("No build-in with ID " + descriptorId + " is supported");
        }
        return type;
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
     * Returns type of the descriptor.
     *
     * @return Type.
     */
    public Class<?> clazz() {
        return clazz;
    }

    /**
     * Creates a descriptor for this built-in type.
     *
     * @return Descriptor.
     */
    public ClassDescriptor asClassDescriptor() {
        ClassDescriptor componentTypeDescriptor = null;
        if (clazz.isArray()) {
            BuiltInType componentType = classToValues.get(clazz.getComponentType());
            if (componentType == null) {
                throw new IllegalStateException("Component of a built-in array type is not built-in: " + clazz);
            }
            componentTypeDescriptor = componentType.asClassDescriptor();
        }

        return ClassDescriptor.forLocal(
                clazz,
                descriptorId,
                null,
                componentTypeDescriptor,
                Collections.emptyList(),
                new Serialization(SerializationType.BUILTIN)
        );
    }

    private static class ReferencePlaceholder {
    }

    private static class StringLatin1Placeholder {
    }
}
