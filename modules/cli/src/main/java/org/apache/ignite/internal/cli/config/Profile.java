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

package org.apache.ignite.internal.cli.config;

import java.util.Map;

/**
 * Ignite CLI Profile.
 */
public interface Profile {
    /**
     * Gets name.
     *
     * @return profile name
     */
    String getName();

    /**
     * Gets all properties.
     *
     * @return map of all properties
     */
    Map<String, String> getAll();

    /**
     * Gets a property.
     *
     * @param key property to get
     * @return property value or {@code null} if config doesn't contain this property
     */
    String getProperty(String key);

    /**
     * Gets a property.
     *
     * @param key property to get
     * @param defaultValue default value of the property
     *
     * @return property value or {@code defaultValue} if config doesn't contain this property
     */
    String getProperty(String key, String defaultValue);

    /**
     * Sets a property.
     *
     * @param key property to set
     * @param value value to set
     */
    void setProperty(String key, String value);

    /**
     * Removes a property.
     *
     * @param key property to remove
     * @return removed property, or {@code null} if there was no such property.
     */
    String removeProperty(String key);

    /**
     * Set properties.
     *
     * @param values map of properties to set
     */
    void setProperties(Map<String, String> values);
}
