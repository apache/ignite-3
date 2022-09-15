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

package org.apache.ignite.internal.sql.engine.property;

import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * An object that keeps values of the properties.
 *
 * @see Property
 */
public interface PropertiesHolder {
    /**
     * Returns the value for specified property or {@code null} if such property is not specified in the current holder.
     *
     * @param prop Property of interest.
     * @param <T> Value type of the property.
     * @return The value of the property or {@code null} if such property is not specified in the current holder.
     */
    <T> @Nullable T get(Property<T> prop);

    /**
     * Returns the value for specified property or provided default (which in turn may be null as well) if such property is not
     * specified in the current holder.
     *
     * @param prop Property of interest.
     * @param <T> Value type of the property.
     * @return The value of the property or provided default if such property is not specified in the current holder.
     */

    <T> @Nullable T getOrDefault(Property<T> prop, @Nullable T defaultValue);

    /**
     * Returns the number of values contained in the holder.
     *
     * @return The number of values contained in the holder.
     */
    int size();

    /**
     * Creates a mapping from the current holder.
     *
     * @return Mapping of the property to its value.
     */
    Map<Property<?>, Object> toMap();

    /**
     * Returns a holder created from a provided mapping.
     *
     * @param values Values to hold.
     * @return Holder.
     */
    static PropertiesHolder fromMap(Map<Property<?>, Object> values) {
        for (Map.Entry<Property<?>, Object> e : values.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }

            if (!e.getKey().cls.isAssignableFrom(e.getValue().getClass())) {
                throw new IllegalArgumentException();
            }
        }

        return new PropertiesHolderImpl(Map.copyOf(values));
    }
}
