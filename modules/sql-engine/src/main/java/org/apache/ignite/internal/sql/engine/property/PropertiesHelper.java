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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.sql.engine.QueryProperty;


/**
 * Utility class to work with {@link Property}.
 */
public final class PropertiesHelper {
    private PropertiesHelper() {}

    /**
     * Collects all the {@link Property properties} which are defined as a public static field within the specified class.
     *
     * @param cls The class for property lookup.
     * @return A mapping name to property itself.
     */
    public static Map<String, Property<?>> createPropsByNameMap(Class<?> cls) {
        Map<String, Property<?>> tmp = new HashMap<>();

        for (Field f : cls.getDeclaredFields()) {
            if (!Property.class.equals(f.getType())
                    || !Modifier.isStatic(f.getModifiers())
                    || !Modifier.isPublic(f.getModifiers())) {
                continue;
            }

            try {
                Property<?> prop = (Property<?>) f.get(QueryProperty.class);

                tmp.put(prop.name, prop);
            } catch (IllegalAccessException e) {
                // should not happen
                throw new AssertionError(e);
            }
        }

        return Map.copyOf(tmp);
    }
}
