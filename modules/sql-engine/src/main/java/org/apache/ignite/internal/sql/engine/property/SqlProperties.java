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

/**
 * An object that keeps values of the properties.
 *
 * @see Property
 */
public interface SqlProperties extends Iterable<Map.Entry<Property<?>, Object>> {
    /**
     * Returns the value for specified property or throws an {@link PropertyNotFoundException}
     * if such property is not specified.
     *
     * @param prop Property of interest.
     * @param <T> Value type of the property.
     * @return The value of the property. Never returns null.
     * @throws PropertyNotFoundException If a given property is not found.
     */
    <T> T get(Property<T> prop);

    /**
     * Returns the value for specified property or provided default if such property is not specified.
     *
     * @param prop Property of interest.
     * @param defaultValue A value to return if given property not found.
     * @param <T> Value type of the property.
     * @return The value of the property or provided default if such property is not specified.
     */
    <T> T getOrDefault(Property<T> prop, T defaultValue);

    /**
     * Returns {@code true} if properties object contains given property.
     *
     * @param prop A property of interest.
     * @return {@code true} if properties object contains given property.
     */
    boolean hasProperty(Property<?> prop);

    /**
     * A builder interface to constructs the properties object.
     */
    interface Builder {
        /**
         * Stores the value of a given property.
         *
         * <p>If such property was set earlier, then replace the old value with the new one.
         *
         * @param property Property to set.
         * @param value Value to set property with.
         * @param <T> A type of the value.
         * @return {@code this} for chaining.
         * @throws IllegalArgumentException if value type doesn't match the type of property.
         */
        <T> Builder set(Property<T> property, T value);

        /** Creates a properties object. */
        SqlProperties build();
    }
}
