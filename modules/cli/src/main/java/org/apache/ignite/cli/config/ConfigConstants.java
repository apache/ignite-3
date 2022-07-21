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

import java.io.File;
import java.nio.file.Path;

/**
 * Util class for config CLI.
 */
public final class ConfigConstants {
    private static final String XDG_CONFIG_HOME = "XDG_CONFIG_HOME";
    private static final String PARENT_FOLDER_NAME = "ignitecli";
    private static final String CONFIG_FILE_NAME = "defaults";

    public static final String CURRENT_PROFILE = "current_profile";
    public static final String CLUSTER_URL = "ignite.cluster-url";
    public static final String JDBC_URL = "ignite.jdbc-url";
    public static final String LAST_CONNECTED_URL = "ignite.last-connected-url";

    private ConfigConstants() {

    }


    public static File getConfigFile() {
        return getConfigRoot().resolve(PARENT_FOLDER_NAME).resolve(CONFIG_FILE_NAME).toFile();
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
