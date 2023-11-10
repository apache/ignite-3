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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.sql.engine.property.SqlProperties.Builder;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.FilteringIterator;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Utility class to work with {@link Property}.
 */
public final class SqlPropertiesHelper {
    private SqlPropertiesHelper() {
        throw new IllegalStateException();
    }

    /** Creates empty properties object. */
    public static SqlProperties emptyProperties() {
        return new SqlPropertiesImpl(Map.of());
    }

    /** Creates new builder. */
    public static Builder newBuilder() {
        return new BuilderImpl();
    }

    /**
     * Merges two properties objects into single one.
     *
     * <p>If conflict arises, conflicted values of secondary object will be overridden with values
     * of primary object.
     *
     * @param primary A properties whose values will be preserved.
     * @param secondary A properties whose values will be overridden in case of conflict.
     * @return A properties object containing properties from both objects.
     */
    public static SqlProperties merge(SqlProperties primary, SqlProperties secondary) {
        Builder builder = builderFromProperties(secondary);

        for (Map.Entry<Property<?>, Object> entry : primary) {
            builder.set((Property<Object>) entry.getKey(), entry.getValue());
        }

        return builder.build();
    }

    /**
     * Creates a chained properties object. That is, on every access to the property it will look up
     * certain property in the primary object, and then, if property was not found, in secondary.
     *
     * @param primary A primary object.
     * @param secondary A secondary object.
     * @return A chained properties object.
     */
    public static SqlProperties chain(SqlProperties primary, SqlProperties secondary) {
        return new ChainedSqlProperties(primary, secondary);
    }

    /**
     * Creates new builder and populates it with properties from the given properties object.
     *
     * @param holder A holder to populate new builder with.
     * @return A builder containing properties from given properties object.
     */
    public static Builder builderFromProperties(SqlProperties holder) {
        Builder builder = new BuilderImpl();

        for (Map.Entry<Property<?>, Object> e : holder) {
            builder.set((Property<Object>) e.getKey(), e.getValue());
        }

        return builder;
    }

    /**
     * Collects all the {@link Property properties} which are defined as a public static field within the specified class.
     *
     * @param cls The class for property lookup.
     * @return A mapping name to property itself.
     */
    @SuppressWarnings("rawtypes")
    public static Map<String, Property<?>> createPropsByNameMap(Class<?> cls) {
        List<Property> properties = IgniteUtils.collectStaticFields(cls, Property.class);

        Map<String, Property<?>> tmp = new HashMap<>();

        for (Property<?> property : properties) {
            tmp.put(property.name, property);
        }

        return Map.copyOf(tmp);
    }

    private static class ChainedSqlProperties implements SqlProperties {
        private final SqlProperties primary;
        private final SqlProperties secondary;

        private ChainedSqlProperties(SqlProperties primary, SqlProperties secondary) {
            this.primary = primary;
            this.secondary = secondary;
        }

        @Override
        public <T> T get(Property<T> prop) {
            if (primary.hasProperty(prop)) {
                return primary.get(prop);
            }

            return secondary.get(prop);
        }

        @Override
        public <T> T getOrDefault(Property<T> prop, T defaultValue) {
            if (primary.hasProperty(prop)) {
                return primary.get(prop);
            }

            return secondary.getOrDefault(prop, defaultValue);
        }

        @Override
        public boolean hasProperty(Property<?> prop) {
            return primary.hasProperty(prop) || secondary.hasProperty(prop);
        }

        @Override
        public Iterator<Entry<Property<?>, Object>> iterator() {
            return CollectionUtils.concat(
                    primary.iterator(),
                    new FilteringIterator<>(secondary.iterator(), entry -> !primary.hasProperty(entry.getKey()))
            );
        }
    }
}
