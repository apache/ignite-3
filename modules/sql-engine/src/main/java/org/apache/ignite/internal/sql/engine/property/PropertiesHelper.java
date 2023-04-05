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
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder.Builder;
import org.apache.ignite.internal.util.IgniteUtils;


/**
 * Utility class to work with {@link Property}.
 */
public final class PropertiesHelper {
    private PropertiesHelper() {
        throw new IllegalStateException();
    }

    /** Creates empty holder. */
    public static PropertiesHolder emptyHolder() {
        return new PropertiesHolderImpl(Map.of());
    }

    /** Creates new builder. */
    public static Builder newBuilder() {
        return new BuilderImpl();
    }

    /**
     * Merges two holder into single one.
     *
     * <p>If conflict arises, conflicted values of secondary holder will be overridden with values
     * of primary holder.
     *
     * @param primaryHolder A holder whose values will be preserved.
     * @param secondaryHolder A holder whose values will be overridden in case of conflict.
     * @return A holder containing properties from both holders.
     */
    public static PropertiesHolder merge(PropertiesHolder primaryHolder, PropertiesHolder secondaryHolder) {
        Builder builder = builderFromHolder(secondaryHolder);

        for (Map.Entry<Property<?>, Object> entry : primaryHolder) {
            builder.set((Property<Object>) entry.getKey(), entry.getValue());
        }

        return builder.build();
    }

    /**
     * Creates new builder and populates it with properties from the given holder.
     *
     * @param holder A holder to populate new builder with.
     * @return A builder containing properties from given holder.
     */
    public static Builder builderFromHolder(PropertiesHolder holder) {
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
}
