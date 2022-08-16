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
    /**
     * Gets name of the profile.
     *
     * @return profile name
     */
    String getName();

    /**
     * Gets a {@link Config} stored in this profile.
     *
     * @return config
     */
    Config getConfig();

    /**
     * Convenience method to get all properties from this profile.
     *
     * @return map of all properties
     */
    default Map<String, String> getAll() {
        return getConfig().getAll();
    }

    /**
     * Convenience method to get a property from this profile.
     *
     * @param key property to get
     * @return property value or {@code null} if config doesn't contain this property
     */
    default String getProperty(String key) {
        return getConfig().getProperty(key);
    }

    /**
     * Convenience method to get a property from this profile.
     *
     * @param key property to get
     * @param defaultValue default value of the property
     *
     * @return property value or {@code defaultValue} if config doesn't contain this property
     */
    default String getProperty(String key, String defaultValue) {
        return getConfig().getProperty(key, defaultValue);
    }

    /**
     * Convenience method to set a property to this profile.
     *
     * @param key property to set
     * @param value value to set
     */
    default void setProperty(String key, String value) {
        getConfig().setProperty(key, value);
    }

    /**
     * Convenience method to set properties to this profile.
     *
     * @param values map of properties to set
     */
    default void setProperties(Map<String, String> values) {
        getConfig().setProperties(values);
    }
}
