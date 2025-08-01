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

package org.apache.ignite.internal.tostring;

import static java.lang.reflect.Modifier.isStatic;
import static org.apache.ignite.internal.tostring.ToStringUtils.createStringifier;

import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

/**
 * Simple field descriptor containing field name and its order in the class descriptor.
 */
class FieldDescriptor {
    /** Field type: {@link Object}. */
    static final int FIELD_TYPE_OBJECT = 0;

    /** Field type: {@code byte}. */
    static final int FIELD_TYPE_BYTE = 1;

    /** Field type: {@code boolean}. */
    static final int FIELD_TYPE_BOOLEAN = 2;

    /** Field type: {@code char}. */
    static final int FIELD_TYPE_CHAR = 3;

    /** Field type: {@code short}. */
    static final int FIELD_TYPE_SHORT = 4;

    /** Field type: {@code int}. */
    static final int FIELD_TYPE_INT = 5;

    /** Field type: {@code float}. */
    static final int FIELD_TYPE_FLOAT = 6;

    /** Field type: {@code long}. */
    static final int FIELD_TYPE_LONG = 7;

    /** Field type: {@code double}. */
    static final int FIELD_TYPE_DOUBLE = 8;

    /** Field name. */
    private final String name;

    /** Field order. */
    private final int order;

    /** Field VarHandle. */
    private final VarHandle varHandle;

    /** Numeric constant for the field's type. One of {@code FIELD_TYPE_*} constants of current class. */
    private final int type;

    /** Class of the field. Upper bound in case of generic field types. */
    private final Class<?> cls;

    /** Field Stringifier, {@code null} if absent. */
    private final @Nullable Stringifier<?> stringifier;

    /**
     * Constructor.
     *
     * @param field Field descriptor.
     * @param varHandle Field VarHandle.
     */
    FieldDescriptor(Field field, VarHandle varHandle) {
        assert !isStatic(field.getModifiers()) : "Static fields are not allowed here: " + field;

        this.varHandle = varHandle;

        cls = field.getType();

        order = getFieldOrder(field);

        IgniteStringifier igniteStringifier = field.getAnnotation(IgniteStringifier.class);

        if (igniteStringifier == null) {
            name = field.getName();
            type = getIntFieldType(field);
            stringifier = null;
        } else {
            name = "".equals(igniteStringifier.name()) ? field.getName() : igniteStringifier.name();
            type = FIELD_TYPE_OBJECT;
            stringifier = createStringifier(igniteStringifier.value());
        }
    }

    /** Returns field order. */
    int getOrder() {
        return order;
    }

    /** Returns field VarHandle. */
    VarHandle varHandle() {
        return varHandle;
    }

    /** Returns numeric constant for the field's type. One of {@code FIELD_TYPE_*} constants of current class. */
    @MagicConstant(valuesFromClass = FieldDescriptor.class)
    int type() {
        return type;
    }

    /** Returns field class. */
    Class<?> fieldClass() {
        return cls;
    }

    /** Returns field name. */
    String getName() {
        return name;
    }

    /** Returns field Stringifier, {@code null} if absent. */
    @Nullable Stringifier<?> stringifier() {
        return stringifier;
    }

    private static int getIntFieldType(Field field) {
        Class<?> cls = field.getType();

        if (!cls.isPrimitive()) {
            return FIELD_TYPE_OBJECT;
        } else if (cls == byte.class) {
            return FIELD_TYPE_BYTE;
        } else if (cls == boolean.class) {
            return FIELD_TYPE_BOOLEAN;
        } else if (cls == char.class) {
            return FIELD_TYPE_CHAR;
        } else if (cls == short.class) {
            return FIELD_TYPE_SHORT;
        } else if (cls == int.class) {
            return FIELD_TYPE_INT;
        } else if (cls == float.class) {
            return FIELD_TYPE_FLOAT;
        } else if (cls == long.class) {
            return FIELD_TYPE_LONG;
        } else if (cls == double.class) {
            return FIELD_TYPE_DOUBLE;
        }

        throw new IllegalArgumentException("Unexpected type: " + field);
    }

    private static int getFieldOrder(Field field) {
        IgniteToStringOrder annotation = field.getAnnotation(IgniteToStringOrder.class);

        return annotation == null ? Integer.MAX_VALUE : annotation.value();
    }
}
