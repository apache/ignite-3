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

import java.io.File;
import java.nio.file.Path;

/**
 * Util class for config CLI.
 */
public final class ConfigConstants {
    /**
     * Environment variable which points to the base directory for configuration files according to XDG spec.
     */
    private static final String XDG_CONFIG_HOME = "XDG_CONFIG_HOME";

    /**
     * Subdirectory name under the base configuration directory for ignite configuration files.
     */
    private static final String PARENT_FOLDER_NAME = "ignitecli";

    /**
     * Main configuration file name.
     */
    private static final String CONFIG_FILE_NAME = "defaults";

    /**
     * Secret configuration file name.
     */
    private static final String SECRET_CONFIG_FILE_NAME = "secrets";

    /**
     * Environment variable which points to the configuration file.
     */
    private static final String IGNITE_CLI_CONFIG_FILE = "IGNITE_CLI_CONFIG_FILE";

    /**
     * Environment variable which points to the secret configuration file.
     */
    private static final String IGNITE_CLI_SECRET_CONFIG_FILE = "IGNITE_CLI_SECRET_CONFIG_FILE";

    /**
     * Environment variable which points to the CLI logs folder.
     */
    public static final String IGNITE_CLI_LOGS_DIR = "IGNITE_CLI_LOGS_DIR";

    /**
     * Current profile property name.
     */
    public static final String CURRENT_PROFILE = "current_profile";

    private ConfigConstants() {
    }

    /**
     * Gets the {@link File} with user-specific configuration file. The file location can be overridden using
     * {@link ConfigConstants#IGNITE_CLI_CONFIG_FILE} environment variable, otherwise base directory is specified by the
     * <a href="https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html">XDG Base Directory Specification</a>
     * and the configuration file name is {@code ignitecli/defaults} under the base directory.
     *
     * @return configuration file.
     */
    public static File getConfigFile() {
        String configFile = System.getenv(IGNITE_CLI_CONFIG_FILE);
        if (configFile != null) {
            return new File(configFile);
        }
        return getConfigRoot().resolve(PARENT_FOLDER_NAME).resolve(CONFIG_FILE_NAME).toFile();
    }

    /**
     * Gets the {@link File} with user-specific configuration file. The file location can be overridden using
     * {@link ConfigConstants#IGNITE_CLI_SECRET_CONFIG_FILE} environment variable, otherwise base directory is specified by the
     * <a href="https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html">XDG Base Directory Specification</a>
     * and the configuration file name is {@code ignitecli/secrets} under the base directory.
     *
     * @return configuration file.
     */
    public static File getSecretConfigFile() {
        String configFile = System.getenv(IGNITE_CLI_SECRET_CONFIG_FILE);
        if (configFile != null) {
            return new File(configFile);
        }
        return getConfigRoot().resolve(PARENT_FOLDER_NAME).resolve(SECRET_CONFIG_FILE_NAME).toFile();
    }

    private static Path getConfigRoot() {
        String xdgConfigHome = System.getenv(XDG_CONFIG_HOME);
        if (xdgConfigHome != null) {
            return Path.of(xdgConfigHome);
        } else {
            return Path.of(System.getProperty("user.home"), ".config");
        }
    }
}
