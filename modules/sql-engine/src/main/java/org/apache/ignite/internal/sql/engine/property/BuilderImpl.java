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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder.Builder;

/**
 * Implementation of the builder interface to constructs the holder with
 * given properties.
 */
class BuilderImpl implements Builder {
    private final Map<Property<?>, Object> properties = new HashMap<>();

    /** {@inheritDoc} */
    @Override
    public <T> Builder set(Property<T> property, T value) {
        Objects.requireNonNull(value, "value");

        if (!property.cls.isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException(
                    format("Unable to assign value of type \"{}\" to property {}", value.getClass().getName(), property)
            );
        }

        properties.put(property, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public PropertiesHolder build() {
        return new PropertiesHolderImpl(Map.copyOf(properties));
    }
}
