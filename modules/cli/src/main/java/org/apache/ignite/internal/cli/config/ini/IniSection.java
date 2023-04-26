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

package org.apache.ignite.internal.cli.config.ini;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * INI section representation.
 */
public class IniSection {
    private final String name;
    private final Map<String, String> props = new LinkedHashMap<>();

    public IniSection(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(props);
    }

    public String getProperty(String key) {
        return props.get(key);
    }

    public String getProperty(String key, String defaultValue) {
        return props.getOrDefault(key, defaultValue);
    }

    public void setProperty(String key, String value) {
        props.put(key, value);
    }

    public String removeProperty(String key) {
        return props.remove(key);
    }

    public void setProperties(Map<String, String> values) {
        props.putAll(values);
    }
}
