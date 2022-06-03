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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Properties;

/**
 * CLI default configuration.
 */
public class Config {
    private static final String XDG_CONFIG_HOME = "XDG_CONFIG_HOME";
    private static final String XDG_STATE_HOME = "XDG_STATE_HOME";
    private static final String PARENT_FOLDER_NAME = "ignitecli";
    private static final String CONFIG_FILE_NAME = "defaults";

    private final File configFile;
    private final Properties props = new Properties();

    public Config(File configFile) {
        this.configFile = configFile;
        loadConfig(configFile);
    }

    /**
     * Loads config from the default location specified by the XDG_CONFIG_HOME.
     */
    public Config() {
        this(getConfigFile());
    }

    public Properties getProperties() {
        return props;
    }

    public String getProperty(String key) {
        return props.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    public void setProperty(String key, String value) {
        props.setProperty(key, value);
    }

    private void loadConfig(File configFile) {
        if (configFile.canRead()) {
            try (InputStream is = new FileInputStream(configFile)) {
                props.load(is);
            } catch (IOException e) {
                // todo report error?
            }
        }
    }

    /**
     * Saves config to file.
     */
    public void saveConfig() {
        configFile.getParentFile().mkdirs();
        if (configFile.canWrite()) {
            try (OutputStream os = new FileOutputStream(configFile)) {
                props.store(os, null);
            } catch (IOException e) {
                // todo report error?
            }
        }
    }

    private static File getConfigFile() {
        return getConfigRoot().resolve(PARENT_FOLDER_NAME).resolve(CONFIG_FILE_NAME).toFile();
    }

    /**
     * Gets the path for the state.
     *
     * @return Folder for state storage.
     */
    public static File getStateFolder() {
        return getStateRoot().resolve(PARENT_FOLDER_NAME).toFile();
    }

    private static Path getConfigRoot() {
        String xdgConfigHome = System.getenv(XDG_CONFIG_HOME);
        if (xdgConfigHome != null) {
            return Path.of(xdgConfigHome);
        } else {
            return Path.of(System.getProperty("user.home"), ".config");
        }
    }

    private static Path getStateRoot() {
        String xdgStateHome = System.getenv(XDG_STATE_HOME);
        if (xdgStateHome != null) {
            return Path.of(xdgStateHome);
        } else {
            return Path.of(System.getProperty("user.home"), ".local", "state");
        }
    }
}
