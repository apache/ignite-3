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

package org.apache.ignite.internal.configuration.hocon;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import org.apache.ignite.internal.configuration.TypeUtils;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;

import static com.typesafe.config.ConfigValueType.BOOLEAN;
import static com.typesafe.config.ConfigValueType.LIST;
import static com.typesafe.config.ConfigValueType.NUMBER;
import static com.typesafe.config.ConfigValueType.STRING;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.join;

/**
 * {@link ConfigurationSource} created from a HOCON element representing a primitive type.
 */
class HoconPrimitiveConfigurationSource implements ConfigurationSource {
    /**
     * Current path inside the top-level HOCON object.
     */
    private final List<String> path;

    /**
     * HOCON object that this source has been created from.
     */
    private final ConfigValue hoconCfgValue;

    /**
     * Creates a {@link ConfigurationSource} from the given HOCON object representing a primitive type.
     *
     * @param path current path inside the top-level HOCON object. Can be empty if the given {@code hoconCfgValue}
     *             is the top-level object
     * @param hoconCfgValue HOCON object
     */
    HoconPrimitiveConfigurationSource(List<String> path, ConfigValue hoconCfgValue) {
        assert !path.isEmpty();

        this.path = path;
        this.hoconCfgValue = hoconCfgValue;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isArray()) {
            if (hoconCfgValue.valueType() != LIST)
                throw wrongTypeException(clazz, -1);

            ConfigList hoconCfgList = (ConfigList)hoconCfgValue;
            int size = hoconCfgList.size();

            Class<?> componentType = clazz.getComponentType();
            Class<?> boxedComponentType = box(componentType);

            Object resArray = Array.newInstance(componentType, size);

            int idx = 0;
            for (Iterator<ConfigValue> iterator = hoconCfgList.iterator(); iterator.hasNext(); idx++) {
                ConfigValue hoconCfgListElement = iterator.next();

                switch (hoconCfgListElement.valueType()) {
                    case OBJECT:
                    case LIST:
                        throw wrongTypeException(boxedComponentType, idx);

                    default:
                        Array.set(resArray, idx, unwrap(hoconCfgListElement, boxedComponentType, idx));
                }
            }

            return (T)resArray;
        }
        else {
            if (hoconCfgValue.valueType() == LIST)
                throw wrongTypeException(clazz, -1);

            return unwrap(hoconCfgValue, clazz, -1);
        }
    }

    /** {@inheritDoc} */
    @Override public void descend(ConstructableTreeNode node) {
        throw new IllegalArgumentException(
            "'" + join(path) + "' is expected to be a composite configuration node, not a single value"
        );
    }

    /** */
    private IllegalArgumentException wrongTypeException(Class<?> clazz, int idx) {
        return new IllegalArgumentException(String.format(
            "'%s' is expected as a type for the '%s' configuration value",
            unbox(clazz).getSimpleName(), formatArrayPath(idx)
        ));
    }

    /**
     * Non-null wrapper over {@link TypeUtils#boxed}.
     */
    private static Class<?> box(Class<?> clazz) {
        Class<?> boxed = TypeUtils.boxed(clazz);

        return boxed == null ? clazz : boxed;
    }

    /**
     * Non-null wrapper over {@link TypeUtils#unboxed}.
     */
    private static Class<?> unbox(Class<?> clazz) {
        assert !clazz.isPrimitive();

        Class<?> unboxed = TypeUtils.unboxed(clazz);

        return unboxed == null ? clazz : unboxed;
    }

    /**
     * Extracts the value from the given {@link ConfigValue} based on the expected type.
     */
    private <T> T unwrap(ConfigValue hoconCfgValue, Class<T> clazz, int idx) {
        assert !clazz.isArray();
        assert !clazz.isPrimitive();

        if (clazz == String.class) {
            if (hoconCfgValue.valueType() != STRING)
                throw wrongTypeException(clazz, idx);

            return clazz.cast(hoconCfgValue.unwrapped());
        }
        else if (clazz == Boolean.class) {
            if (hoconCfgValue.valueType() != BOOLEAN)
                throw wrongTypeException(clazz, idx);

            return clazz.cast(hoconCfgValue.unwrapped());
        }
        else if (clazz == Character.class) {
            if (hoconCfgValue.valueType() != STRING || hoconCfgValue.unwrapped().toString().length() != 1)
                throw wrongTypeException(clazz, idx);

            return clazz.cast(hoconCfgValue.unwrapped().toString().charAt(0));
        }
        else if (Number.class.isAssignableFrom(clazz)) {
            if (hoconCfgValue.valueType() != NUMBER)
                throw wrongTypeException(clazz, idx);

            Number numberValue = (Number)hoconCfgValue.unwrapped();

            if (clazz == Byte.class) {
                checkBounds(numberValue, Byte.MIN_VALUE, Byte.MAX_VALUE, idx);

                return clazz.cast(numberValue.byteValue());
            }
            else if (clazz == Short.class) {
                checkBounds(numberValue, Short.MIN_VALUE, Short.MAX_VALUE, idx);

                return clazz.cast(numberValue.shortValue());
            }
            else if (clazz == Integer.class) {
                checkBounds(numberValue, Integer.MIN_VALUE, Integer.MAX_VALUE, idx);

                return clazz.cast(numberValue.intValue());
            }
            else if (clazz == Long.class)
                return clazz.cast(numberValue.longValue());
            else if (clazz == Float.class)
                return clazz.cast(numberValue.floatValue());
            else if (clazz == Double.class)
                return clazz.cast(numberValue.doubleValue());
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    /**
     * Checks that a number fits into the given bounds.
     */
    private void checkBounds(Number numberValue, long lower, long upper, int idx) {
        long longValue = numberValue.longValue();

        if (longValue < lower || longValue > upper) {
            throw new IllegalArgumentException(String.format(
                "Value '%d' of '%s' is out of its declared bounds: [%d : %d]",
                longValue, formatArrayPath(idx), lower, upper
            ));
        }
    }

    /**
     * Creates a string representation of the current HOCON path inside of an array.
     */
    private String formatArrayPath(int idx) {
        return join(path) + (idx == -1 ? "" : ("[" + idx + "]"));
    }
}
