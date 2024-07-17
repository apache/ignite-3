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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.Profile;

/**
 * Implementation of {@link Profile} based on {@link IniSection}.
 */
public class IniProfile implements Profile {
    private final String name;
    private final IniConfig config;
    private final IniConfig secretConfig;

    /**
     * Constructor.
     *
     * @param name Profile name.
     * @param config Ini config.
     * @param secretConfig Ini secret config.
     */
    public IniProfile(String name, IniConfig config, IniConfig secretConfig) {
        this.name = name;
        this.config = config;
        this.secretConfig = secretConfig;
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> getAll() {
        Map<String, String> all = new HashMap<>(config.getAll());
        all.putAll(secretConfig.getAll());
        return all;
    }

    /** {@inheritDoc} */
    @Override
    public String getProperty(String key) {
        return getConfig(key).getProperty(key);
    }

    /** {@inheritDoc} */
    @Override
    public String getProperty(String key, String defaultValue) {
        return getConfig(key).getProperty(key, defaultValue);
    }

    /** {@inheritDoc} */
    @Override
    public void setProperty(String key, String value) {
        getConfig(key).setProperty(key, value);
    }

    @Override
    public String removeProperty(String key) {
        return getConfig(key).removeProperty(key);
    }

    /** {@inheritDoc} */
    @Override
    public void setProperties(Map<String, String> values) {
        Map<Boolean, Map<String, String>> secretToValues = values.entrySet().stream()
                .collect(Collectors.groupingBy(it -> CliConfigKeys.secretConfigKeys().contains(it.getKey()),
                        Collectors.toMap(Entry::getKey, Entry::getValue)));

        Map<String, String> configValues = secretToValues.get(false);
        if (configValues != null) {
            config.setProperties(configValues);
        }

        Map<String, String> secretConfigValues = secretToValues.get(true);
        if (secretConfigValues != null) {
            secretConfig.setProperties(secretConfigValues);
        }
    }

    private IniConfig getConfig(String key) {
        return CliConfigKeys.secretConfigKeys().contains(key) ? secretConfig : config;
    }
}
