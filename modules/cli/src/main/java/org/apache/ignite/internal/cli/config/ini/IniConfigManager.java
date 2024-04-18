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

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.CLUSTER_URL;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_CLIENT_AUTH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_TRUST_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_URL;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.SQL_MULTILINE;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.SYNTAX_HIGHLIGHTING;
import static org.apache.ignite.internal.cli.config.ConfigConstants.CURRENT_PROFILE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.Profile;
import org.apache.ignite.internal.cli.config.exception.ConfigInitializationException;
import org.apache.ignite.internal.cli.config.exception.ProfileNotFoundException;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.OperatingSystem;

/**
 * Implementation of {@link ConfigManager} based on {@link IniFile}.
 */
public class IniConfigManager implements ConfigManager {
    private static final IgniteLogger LOG = CliLoggers.forClass(IniConfigManager.class);

    private static final String DEFAULT_PROFILE_NAME = "default";

    private final IniFile configFile;

    private final IniFile secretConfigFile;

    private String currentProfileName;

    /**
     * Constructor.
     *
     * @param configFile ini file.
     * @param secretConfigFile secret ini file.
     */
    public IniConfigManager(File configFile, File secretConfigFile) {
        this.configFile = configFile(configFile);
        this.secretConfigFile = secretConfigFile(secretConfigFile);
        this.currentProfileName = findCurrentProfileName(this.configFile);
    }

    private IniFile configFile(File file) {
        IniFile configFile;
        try {
            configFile = new IniFile(file);
            findCurrentProfileName(configFile);
        } catch (IOException | NoSuchElementException e) {
            LOG.warn("User config is corrupted or doesn't exist.", e);
            try {
                configFile = createDefaultConfig(file);
            } catch (Exception ex) {
                throw new IgniteCliException("Couldn't create default config", ex);
            }
        }
        return configFile;
    }

    private IniFile secretConfigFile(File file) {
        IniFile configFile;
        try {
            if (OperatingSystem.current() != OperatingSystem.WINDOWS) {
                Set<PosixFilePermission> posixFilePermissions = Files.getPosixFilePermissions(file.toPath());
                if (!secretPermission().equals(posixFilePermissions)) {
                    throw new IgniteCliException("The secret configuration file must have 700 permissions");
                }
            }
            configFile = new IniFile(file);
        } catch (IOException e) {
            LOG.warn("User secret config is corrupted or doesn't exist.", e);
            try {
                configFile = createDefaultSecretConfig(file);
            } catch (Exception ex) {
                throw new IgniteCliException("Couldn't create secret default config", ex);
            }
        }
        return configFile;
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

        IniSection secretSection = secretConfigFile.getOrCreateSection(profile);

        IniConfig config = new IniConfig(section, configFile::store);
        IniConfig secretConfig = new IniConfig(secretSection, secretConfigFile::store);
        return new IniProfile(section.getName(), config, secretConfig);
    }

    @Override
    public Profile createProfile(String profileName) {
        IniSection section = configFile.createSection(profileName);
        IniSection secretSection = secretConfigFile.createSection(profileName);

        IniConfig config = new IniConfig(section, configFile::store);
        IniConfig secretConfig = new IniConfig(secretSection, secretConfigFile::store);

        return new IniProfile(profileName, config, secretConfig);
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
            ini.getTopLevelSection().setProperty(CURRENT_PROFILE, DEFAULT_PROFILE_NAME);
            IniSection defaultSection = ini.createSection(DEFAULT_PROFILE_NAME);
            defaultSection.setProperty(CLUSTER_URL.value(), "http://localhost:10300");
            defaultSection.setProperty(JDBC_URL.value(), "jdbc:ignite:thin://127.0.0.1:10800");
            defaultSection.setProperty(SQL_MULTILINE.value(), "true");
            defaultSection.setProperty(SYNTAX_HIGHLIGHTING.value(), "true");
            ini.store();
            return ini;
        } catch (IOException e) {
            throw new ConfigInitializationException(file.getAbsolutePath(), e);
        }
    }

    private static IniFile createDefaultSecretConfig(File file) {
        try {
            file.getParentFile().mkdirs();
            file.delete();

            if (OperatingSystem.current() == OperatingSystem.WINDOWS) {
                Files.createFile(file.toPath());
            } else {
                Files.createFile(file.toPath(), PosixFilePermissions.asFileAttribute(secretPermission()));
            }

            IniFile ini = new IniFile(file);
            IniSection defaultSection = ini.createSection(DEFAULT_PROFILE_NAME);
            defaultSection.setProperty(REST_KEY_STORE_PATH.value(), "");
            defaultSection.setProperty(REST_KEY_STORE_PASSWORD.value(), "");
            defaultSection.setProperty(REST_TRUST_STORE_PATH.value(), "");
            defaultSection.setProperty(REST_TRUST_STORE_PASSWORD.value(), "");
            defaultSection.setProperty(JDBC_KEY_STORE_PATH.value(), "");
            defaultSection.setProperty(JDBC_KEY_STORE_PASSWORD.value(), "");
            defaultSection.setProperty(JDBC_TRUST_STORE_PATH.value(), "");
            defaultSection.setProperty(JDBC_TRUST_STORE_PASSWORD.value(), "");
            defaultSection.setProperty(JDBC_CLIENT_AUTH.value(), "");
            defaultSection.setProperty(BASIC_AUTHENTICATION_USERNAME.value(), "");
            defaultSection.setProperty(BASIC_AUTHENTICATION_PASSWORD.value(), "");
            ini.store();
            return ini;
        } catch (IOException e) {
            throw new ConfigInitializationException(file.getAbsolutePath(), e);
        }
    }

    private static Set<PosixFilePermission> secretPermission() {
        return Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE);
    }
}
