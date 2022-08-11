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

package org.apache.ignite.cli.config.ini;

import static org.apache.ignite.cli.config.ConfigConstants.CLUSTER_URL;
import static org.apache.ignite.cli.config.ConfigConstants.CURRENT_PROFILE;
import static org.apache.ignite.cli.config.ConfigConstants.JDBC_URL;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.ignite.cli.config.ConfigInitializationException;
import org.apache.ignite.cli.config.ConfigManager;
import org.apache.ignite.cli.config.Profile;
import org.apache.ignite.cli.config.ProfileNotFoundException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Implementation of {@link ConfigManager} based on {@link IniFile}.
 */
public class IniConfigManager implements ConfigManager {
    private static final IgniteLogger log = Loggers.forClass(IniConfigManager.class);

    private static final String INTERNAL_SECTION_NAME = "ignitecli_internal";

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
            findCurrentProfile(configFile);
        } catch (IOException | NoSuchElementException e) {
            log.warn("User config is corrupted or doesn't exist.", e);
            configFile = createDefaultConfig(file);
        }
        this.configFile = configFile;
        this.currentProfileName = findCurrentProfile(configFile).getProperty(CURRENT_PROFILE);
    }

    private static IniSection findCurrentProfile(IniFile configFile) {
        IniSection internalSection = configFile.getSection(INTERNAL_SECTION_NAME);
        if (internalSection == null) {
            IniSection section = configFile.getSections().stream()
                    .filter(s -> !s.getName().equals(IniParser.NO_SECTION)) // Don't use top-level section
                    .findFirst()
                    .orElseThrow();
            internalSection = configFile.createSection(INTERNAL_SECTION_NAME);
            internalSection.setProperty(CURRENT_PROFILE, section.getName());
        }
        return internalSection;
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
        configFile.getSection(INTERNAL_SECTION_NAME).setProperty(CURRENT_PROFILE, profileName);
        configFile.store();
    }

    private static IniFile createDefaultConfig(File file) {
        try {
            file.getParentFile().mkdirs();
            file.delete();
            file.createNewFile();
            IniFile ini = new IniFile(file);
            IniSection internal = ini.createSection(INTERNAL_SECTION_NAME);
            internal.setProperty("current_profile", "default");
            IniSection defaultSection = ini.createSection("default");
            defaultSection.setProperty(CLUSTER_URL, "http://localhost:10300");
            defaultSection.setProperty(JDBC_URL, "jdbc:ignite:thin://127.0.0.1:10800");
            ini.store();
            return ini;
        } catch (IOException e) {
            throw new ConfigInitializationException(file.getAbsolutePath(), e);
        }
    }
}
