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

package org.apache.ignite.cli.config;

import java.util.Map;

/**
 * Ignite CLI Profile.
 */
public interface Profile {
    String getName();

    Config getConfig();

    default Map<String, String> getAll() {
        return getConfig().getAll();
    }

    default String getProperty(String key) {
        return getConfig().getProperty(key);
    }

    default String getProperty(String key, String defaultValue) {
        return getConfig().getProperty(key, defaultValue);
    }

    default void setProperty(String key, String value) {
        getConfig().setProperty(key, value);
    }

    default void setProperties(Map<String, String> values) {
        getConfig().setProperties(values);
    }
}
