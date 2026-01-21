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

import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Manager of CLI config.
 */
public interface ConfigManager {
    Profile getCurrentProfile();

    Profile getProfile(String profile);

    Profile createProfile(String profileName);

    void setCurrentProfile(String profileName);

    Collection<String> getProfileNames();

    default String getCurrentProperty(String key) {
        return getCurrentProfile().getProperty(key);
    }

    default void setProperty(String key, String value) {
        getCurrentProfile().setProperty(key, value);
    }

    default String getProperty(String key, @Nullable String profileName) {
        return getConfig(profileName).getProperty(key);
    }

    default String getProperty(String key, @Nullable String profileName, String defaultValue) {
        return getConfig(profileName).getProperty(key, defaultValue);
    }

    default String removeProperty(String key, @Nullable String profileName) {
        return getConfig(profileName).removeProperty(key);
    }

    default String removeCurrentProperty(String key) {
        return getConfig(getCurrentProfile().getName()).removeProperty(key);
    }

    private Profile getConfig(@Nullable String profileName) {
        return profileName == null ? getCurrentProfile() : getProfile(profileName);
    }
}
