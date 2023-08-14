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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.property.PropertiesHelper.createPropsByNameMap;

import java.util.Map;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.property.Property;
import org.jetbrains.annotations.Nullable;

/**
 * Enumerates the properties which might be used during query execution.
 */
public final class QueryProperty {
    public static final Property<Long> QUERY_TIMEOUT = new Property<>("query_timeout", Long.class);
    public static final Property<String> DEFAULT_SCHEMA = new Property<>("default_schema", String.class);

    public static final Property<HybridTimestamp> OBSERVABLE_TIMESTAMP = new Property<>("observable_timestamp", HybridTimestamp.class);

    private static final Map<String, Property<?>> propsByName = createPropsByNameMap(QueryProperty.class);

    /** Returns a property for the given name or {@code null} if there is no property with such name. */
    public static @Nullable Property<?> byName(String name) {
        return propsByName.get(name);
    }
}
