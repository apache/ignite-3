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

import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.intellij.lang.annotations.MagicConstant;

/**
 * Simple field descriptor containing field name and its order in the class descriptor.
 */
class FieldDescriptor {
    /** Field type: {@link Object}. */
    public static final int FIELD_TYPE_OBJECT = 0;

    /** Field type: {@code byte}. */
    public static final int FIELD_TYPE_BYTE = 1;

    /** Field type: {@code boolean}. */
    public static final int FIELD_TYPE_BOOLEAN = 2;

    /** Field type: {@code char}. */
    public static final int FIELD_TYPE_CHAR = 3;

    /** Field type: {@code short}. */
    public static final int FIELD_TYPE_SHORT = 4;

    /** Field type: {@code int}. */
    public static final int FIELD_TYPE_INT = 5;

    /** Field type: {@code float}. */
    public static final int FIELD_TYPE_FLOAT = 6;

    /** Field type: {@code long}. */
    public static final int FIELD_TYPE_LONG = 7;

    /** Field type: {@code double}. */
    public static final int FIELD_TYPE_DOUBLE = 8;

    /** Field name. */
    private final String name;

    /** Field order. */
    private int order = Integer.MAX_VALUE;

    /** Field VarHandle. */
    private final VarHandle varHandle;

    /** Numeric constant for the field's type. One of {@code FIELD_TYPE_*} constants of current class. */
    private final int type;

    /** Class of the field. Upper bound in case of generic field types. */
    private final Class<?> cls;

    /**
     * Constructor.
     *
     * @param field     Field descriptor.
     * @param varHandle Field VarHandle.
     */
    FieldDescriptor(Field field, VarHandle varHandle) {
        assert (field.getModifiers() & Modifier.STATIC) == 0 : "Static fields are not allowed here: " + field;

        this.varHandle = varHandle;

        cls = field.getType();

        name = field.getName();

        if (!cls.isPrimitive()) {
            type = FIELD_TYPE_OBJECT;
        } else {
            if (cls == byte.class) {
                type = FIELD_TYPE_BYTE;
            } else if (cls == boolean.class) {
                type = FIELD_TYPE_BOOLEAN;
            } else if (cls == char.class) {
                type = FIELD_TYPE_CHAR;
            } else if (cls == short.class) {
                type = FIELD_TYPE_SHORT;
            } else if (cls == int.class) {
                type = FIELD_TYPE_INT;
            } else if (cls == float.class) {
                type = FIELD_TYPE_FLOAT;
            } else if (cls == long.class) {
                type = FIELD_TYPE_LONG;
            } else if (cls == double.class) {
                type = FIELD_TYPE_DOUBLE;
            } else {
                throw new IllegalArgumentException("Unexpected primitive type: " + cls);
            }
        }
    }

    /**
     * Returns field order.
     *
     * @return Field order.
     */
    int getOrder() {
        return order;
    }

    /**
     * Sets field order.
     *
     * @param order Field order.
     */
    void setOrder(int order) {
        this.order = order;
    }

    /**
     * Returns field VarHandle.
     *
     * @return Field VarHandle.
     */
    public VarHandle varHandle() {
        return varHandle;
    }

    /**
     * Returns numeric constant for the field's type. One of {@code FIELD_TYPE_*} constants of current class.
     *
     * @return Numeric constant for the field's type. One of {@code FIELD_TYPE_*} constants of current class.
     */
    @MagicConstant(valuesFromClass = FieldDescriptor.class)
    public int type() {
        return type;
    }

    /**
     * Returns field class.
     *
     * @return Field class.
     */
    public Class<?> fieldClass() {
        return cls;
    }

    /**
     * Returns field name.
     *
     * @return Field name.
     */
    String getName() {
        return name;
    }
}
