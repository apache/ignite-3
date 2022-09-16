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

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * A simple implementation backed by unmodifiable map.
 */
class PropertiesHolderImpl implements PropertiesHolder {
    private final Map<Property<?>, Object> props;

    /**
     * Constructor.
     *
     * @param props Properties to hold.
     */
    PropertiesHolderImpl(Map<Property<?>, Object> props) {
        this.props = Map.copyOf(props);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public <T> @Nullable T get(Property<T> prop) {
        Object val = props.get(prop);

        if (val == null) {
            return null;
        }

        assert prop.cls.isAssignableFrom(val.getClass())
                : format("Unexpected property value [name={}, expCls={}, actCls={}, val={}]", prop.name, prop.cls, val.getClass(), val);

        return (T) prop.cls.cast(val);
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T getOrDefault(Property<T> prop, @Nullable T defaultValue) {
        var val = get(prop);

        return val == null ? defaultValue : val;
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return props.size();
    }

    /** {@inheritDoc} */
    @Override
    public Map<Property<?>, Object> toMap() {
        return props;
    }
}
