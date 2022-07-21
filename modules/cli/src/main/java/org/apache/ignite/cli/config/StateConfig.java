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
import java.io.IOException;
import org.apache.ignite.cli.config.ini.IniConfig;
import org.apache.ignite.cli.config.ini.IniFile;

/**
 * Config file which stores information between application restarts, but not part of the config.
 */
public class StateConfig {
    private static final String CONFIG_FILE_NAME = "config.ini";

    /**
     * Returns an instance of {@link Config} holding the properties
     *
     * @return new instance of {@link Config}
     */
    public static Config getStateConfig() {
        IniFile file = getStateConfigIniFile();
        return new IniConfig(file.getTopLevelSection(), file::store);
    }

    private static IniFile getStateConfigIniFile() {
        try {
            return new IniFile(StateFolderProvider.getStateFile(CONFIG_FILE_NAME));
        } catch (IOException e) {
            return createDefaultConfig();
        }
    }

    private static IniFile createDefaultConfig() {
        File configFile = StateFolderProvider.getStateFile(CONFIG_FILE_NAME);
        try {
            configFile.getParentFile().mkdirs();
            configFile.delete();
            configFile.createNewFile();
            IniFile ini = new IniFile(configFile);
            ini.store();
            return ini;
        } catch (IOException e) {
            throw new ConfigInitializationException(configFile.getAbsolutePath(), e);
        }
    }
}
