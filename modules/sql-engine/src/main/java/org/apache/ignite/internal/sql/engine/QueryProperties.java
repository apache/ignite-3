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

package org.apache.ignite.internal.sql.engine;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;

/**
 * Query timeout.
 */
public class QueryProperties {
    /** Instance of empty properties. */
    public static QueryProperties EMPTY_PROPERTIES = new QueryProperties(Collections.emptyMap());

    /** Timeout. */
    private final Map<String, Object> props;

    /**
     * Constructor.
     *
     * @param props Properties map.
     */
    public QueryProperties(Map<String, Object> props) {
        this.props = props;
    }

    /**
     * Return query timeout.
     *
     * @return Default query timeout in the given timeunit.
     */
    public boolean contains(String name) {
        return props.containsKey(name);
    }

    /**
     * Return query timeout.
     *
     * @return Default query timeout in the given timeunit.
     */
    public <T> T value(@NotNull String name, Class<?> cls, T dflt) {
        Object v = props.get(name);

        if (v != null && !cls.isInstance(v)) {
            throw new IgniteInternalException("Invalid property type: [name=" + name + ", val=" + v + ", cls=" + v.getClass() + ']');
        }

        return v != null ? (T) v : dflt;
    }

    /**
     * Return integer property.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @return Default query timeout in the given timeunit.
     */
    public int intValue(@NotNull String name, int dflt) {
        return value(name, Integer.class, dflt);
    }

    /**
     * Return integer property.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @return Default query timeout in the given timeunit.
     */
    public long longValue(@NotNull String name, long dflt) {
        return value(name, Long.class, dflt);
    }

    /**
     * Return integer property.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @return Default query timeout in the given timeunit.
     */
    public boolean booleanValue(@NotNull String name, boolean dflt) {
        return value(name, Boolean.class, dflt);
    }
}
