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

/** CLI config keys and constants. */
public enum CliConfigKeys {

    /** Default cluster or node URL property name. */
    CLUSTER_URL(Constants.CLUSTER_URL),

    /** Last connected cluster or node URL property name. */
    LAST_CONNECTED_URL(Constants.LAST_CONNECTED_URL),

    /** REST trust store path property name. */
    REST_TRUST_STORE_PATH(Constants.REST_TRUST_STORE_PATH),

    /** REST trust store password property name. */
    REST_TRUST_STORE_PASSWORD(Constants.REST_TRUST_STORE_PASSWORD),

    /** REST key store path property name. */
    REST_KEY_STORE_PATH(Constants.REST_KEY_STORE_PATH),

    /** REST key store password property name. */
    REST_KEY_STORE_PASSWORD(Constants.REST_KEY_STORE_PASSWORD),

    /** Default JDBC URL property name. */
    JDBC_URL(Constants.JDBC_URL);

    private final String value;

    public String value() {
        return value;
    }

    /** Constants for CLI config. */
    public static final class Constants {
        public static final String CLUSTER_URL = "ignite.cluster-endpoint-url";

        public static final String LAST_CONNECTED_URL = "ignite.last-connected-url";

        public static final String REST_TRUST_STORE_PATH = "ignite.trust-store.path";

        public static final String REST_TRUST_STORE_PASSWORD = "ignite.trust-store.password";

        public static final String REST_KEY_STORE_PATH = "ignite.key-store.path";

        public static final String REST_KEY_STORE_PASSWORD = "ignite.key-store.password";

        public static final String JDBC_URL = "ignite.jdbc-url";

        /** Environment variable which points to the base directory for configuration files according to XDG spec. */
        private static final String XDG_CONFIG_HOME = "XDG_CONFIG_HOME";

        /** Subdirectory name under the base configuration directory for ignite configuration files. */
        private static final String PARENT_FOLDER_NAME = "ignitecli";

        /** Main configuration file name. */
        private static final String CONFIG_FILE_NAME = "defaults";

        /** Environment variable which points to the configuration file. */
        private static final String IGNITE_CLI_CONFIG_FILE = "IGNITE_CLI_CONFIG_FILE";

        /** Environment variable which points to the CLI logs folder. */
        public static final String IGNITE_CLI_LOGS_DIR = "IGNITE_CLI_LOGS_DIR";

        /** Current profile property name. */
        public static final String CURRENT_PROFILE = "current_profile";
    }

    CliConfigKeys(String value) {
        this.value = value;
    }

    /**
     * Gets the {@link File} with user-specific configuration file.
     * The file location can be overridden using {@code IGNITE_CLI_CONFIG_FILE} environment variable,
     * otherwise base directory is specified by the
     * <a href="https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html">XDG Base Directory Specification</a>
     * and the configuration file name is {@code ignitecli/defaults} under the base directory.
     *
     * @return configuration file.
     */
    public static File getConfigFile() {
        String configFile = System.getenv(Constants.IGNITE_CLI_CONFIG_FILE);
        if (configFile != null) {
            return new File(configFile);
        }
        return getConfigRoot().resolve(Constants.PARENT_FOLDER_NAME).resolve(Constants.CONFIG_FILE_NAME).toFile();
    }

    private static Path getConfigRoot() {
        String xdgConfigHome = System.getenv(Constants.XDG_CONFIG_HOME);
        if (xdgConfigHome != null) {
            return Path.of(xdgConfigHome);
        } else {
            return Path.of(System.getProperty("user.home"), ".config");
        }
    }
}
