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
import java.io.IOException;
import org.apache.ignite.internal.cli.config.exception.ConfigInitializationException;
import org.apache.ignite.internal.cli.config.ini.IniConfig;
import org.apache.ignite.internal.cli.config.ini.IniFile;

/**
 * Config file which stores information between application restarts, but not part of the config.
 */
public class StateConfig {

    /**
     * Returns an instance of {@link Config} holding the properties.
     *
     * @param file INI file.
     * @return new instance of {@link Config}
     */
    public static Config getStateConfig(File file) {
        IniFile iniFile = loadStateConfig(file);
        return new IniConfig(iniFile.getTopLevelSection(), iniFile::store);
    }

    private static IniFile loadStateConfig(File file) {
        try {
            return new IniFile(file);
        } catch (IOException e) {
            return createDefaultConfig(file);
        }
    }

    private static IniFile createDefaultConfig(File file) {
        try {
            file.getParentFile().mkdirs();
            file.delete();
            file.createNewFile();
            IniFile ini = new IniFile(file);
            ini.store();
            return ini;
        } catch (IOException e) {
            throw new ConfigInitializationException(file.getAbsolutePath(), e);
        }
    }
}
