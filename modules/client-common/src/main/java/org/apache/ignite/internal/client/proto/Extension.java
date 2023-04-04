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

package org.apache.ignite.internal.client.proto;

/** Extensions. */
public enum Extension {
    USERNAME("username", String.class),
    PASSWORD("password", String.class),
    ;

    private final String key;

    private final Class<?> valueType;

    Extension(String key, Class<?> valueType) {
        this.key = key;
        this.valueType = valueType;
    }

    /**
     * Finds an extension with a provided key.
     *
     * @param key Key.
     * @return Extension.
     */
    public static Extension fromKey(String key) {
        for (Extension e : values()) {
            if (e.key.equalsIgnoreCase(key)) {
                return e;
            }
        }
        throw new IllegalArgumentException("Unknown extension key: " + key);
    }

    public String key() {
        return key;
    }

    public Class<?> valueType() {
        return valueType;
    }
}
