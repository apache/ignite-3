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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.jetbrains.annotations.Nullable;

/**
 * A simple implementation backed by unmodifiable map.
 */
class SqlPropertiesImpl implements SqlProperties {
    private final Map<Property<?>, Object> props;

    /**
     * Constructor.
     *
     * @param props Properties to hold.
     */
    SqlPropertiesImpl(Map<Property<?>, Object> props) {
        this.props = Map.copyOf(props);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T get(Property<T> prop) {
        T val = get0(prop);

        if (val == null) {
            throw new PropertyNotFoundException(prop);
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T getOrDefault(Property<T> prop, T defaultValue) {
        T val = get0(prop);

        if (val == null) {
            return defaultValue;
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasProperty(Property<?> prop) {
        return props.containsKey(prop);
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Entry<Property<?>, Object>> iterator() {
        return props.entrySet().iterator();
    }

    private <T> @Nullable T get0(Property<T> prop) {
        Object val = props.get(prop);

        if (val == null) {
            return null;
        }

        assert prop.cls.isAssignableFrom(val.getClass())
                : format("Unexpected property value [name={}, expCls={}, actCls={}, val={}]", prop.name, prop.cls, val.getClass(), val);

        return (T) prop.cls.cast(val);
    }
}
