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

import static org.apache.ignite.internal.cli.config.ConfigConstants.CLUSTER_URL;
import static org.apache.ignite.internal.cli.config.ConfigConstants.CURRENT_PROFILE;
import static org.apache.ignite.internal.cli.config.ConfigConstants.JDBC_URL;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.cli.config.ConfigInitializationException;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.Profile;
import org.apache.ignite.internal.cli.config.ProfileNotFoundException;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * Implementation of {@link ConfigManager} based on {@link IniFile}.
 */
public class IniConfigManager implements ConfigManager {
    private static final IgniteLogger LOG = CliLoggers.forClass(IniConfigManager.class);

    private static final String DEFAULT_PROFILE_NAME = "default";

    private final IniFile configFile;

    private String currentProfileName;

    /**
     * Constructor.
     *
     * @param file ini file.
     */
    public IniConfigManager(File file) {
        IniFile configFile;
        try {
            configFile = new IniFile(file);
            findCurrentProfileName(configFile);
        } catch (IOException | NoSuchElementException e) {
            LOG.warn("User config is corrupted or doesn't exist.", e);
            configFile = createDefaultConfig(file);
        }
        this.configFile = configFile;
        this.currentProfileName = findCurrentProfileName(configFile);
    }

    private static String findCurrentProfileName(IniFile configFile) {
        IniSection topLevelSection = configFile.getTopLevelSection();
        // Throw an exception in case there are no sections so that the config is recreated with default parameters
        IniSection section = configFile.getSections().stream().findFirst().orElseThrow();
        String currentProfile = topLevelSection.getProperty(CURRENT_PROFILE);
        if (currentProfile == null) {
            topLevelSection.setProperty(CURRENT_PROFILE, section.getName());
            currentProfile = section.getName();
        }
        return currentProfile;
    }

    @Override
    public Profile getCurrentProfile() {
        return getProfile(currentProfileName);
    }

    @Override
    public Profile getProfile(String profile) {
        IniSection section = configFile.getSection(profile);
        if (section == null) {
            throw new ProfileNotFoundException(profile);
        }
        return new IniProfile(section, configFile::store);
    }

    @Override
    public Profile createProfile(String profileName) {
        return new IniProfile(configFile.createSection(profileName), configFile::store);
    }

    @Override
    public void setCurrentProfile(String profileName) {
        IniSection section = configFile.getSection(profileName);
        if (section == null) {
            throw new ProfileNotFoundException(profileName);
        }
        currentProfileName = profileName;
        configFile.getTopLevelSection().setProperty(CURRENT_PROFILE, profileName);
        configFile.store();
    }

    @Override
    public Collection<String> getProfileNames() {
        return configFile.getSectionNames();
    }

    private static IniFile createDefaultConfig(File file) {
        try {
            file.getParentFile().mkdirs();
            file.delete();
            file.createNewFile();
            IniFile ini = new IniFile(file);
            ini.getTopLevelSection().setProperty("current_profile", DEFAULT_PROFILE_NAME);
            IniSection defaultSection = ini.createSection(DEFAULT_PROFILE_NAME);
            defaultSection.setProperty(CLUSTER_URL, "http://localhost:10300");
            defaultSection.setProperty(JDBC_URL, "jdbc:ignite:thin://127.0.0.1:10800");
            ini.store();
            return ini;
        } catch (IOException e) {
            throw new ConfigInitializationException(file.getAbsolutePath(), e);
        }
    }
}
