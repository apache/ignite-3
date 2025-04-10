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

package org.apache.ignite.migrationtools.utils;

import java.util.Map;

/** Small utilities for handling class names. */
public final class ClassnameUtils {

    public static final Map<String, String> PRIMITIVE_TO_WRAPPER = Map.of(
            boolean.class.getName(), Boolean.class.getName(),
            byte.class.getName(), Byte.class.getName(),
            char.class.getName(), Character.class.getName(),
            short.class.getName(), Short.class.getName(),
            int.class.getName(), Integer.class.getName(),
            long.class.getName(), Long.class.getName(),
            double.class.getName(), Double.class.getName(),
            float.class.getName(), Float.class.getName(),
            void.class.getName(), Void.class.getName()
    );

    private ClassnameUtils() {
        // Intentionally left blank
    }

    public static String ensureWrapper(String className) {
        return PRIMITIVE_TO_WRAPPER.getOrDefault(className, className);
    }
}
