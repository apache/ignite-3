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

package org.apache.ignite.internal.cli.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.ignite.internal.cli.commands.SpacedParameterMixin;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;

/**
 * Utility class for config handling.
 */
public class ConfigUtils {
    /**
     * Merge config from file and from CLI if both provided, config from CLI overrides config from file.
     * Otherwise, returns provided non-null config.
     *
     * @param configFile File with config
     * @param config Config from command line.
     * @return String representation of the config.
     */
    public static String formUpdateConfig(File configFile, SpacedParameterMixin config) {
        String resultingConfig = configFile != null ? readConfigFile(configFile, config) : config.toString();

        // Merge config from file and config from command line if they are both provided
        if (configFile != null && config.args() != null && !config.toString().isEmpty()) {
            Config mergedConfig = ConfigFactory.parseString(config.toString())
                    .withFallback(ConfigFactory.parseFile(configFile))
                    .resolve();

            resultingConfig = mergedConfig.root().render();
        }

        return resultingConfig;
    }

    private static String readConfigFile(File configFile, SpacedParameterMixin config) {
        try {
            return Files.readString(configFile.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IgniteCliException("File [" + configFile.getAbsolutePath() + "] not found");
        }
    }
}
