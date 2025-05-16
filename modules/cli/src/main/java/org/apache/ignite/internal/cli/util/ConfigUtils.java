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
import com.typesafe.config.ConfigRenderOptions;
import java.io.File;
import org.apache.ignite.internal.cli.commands.SpacedParameterMixin;

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
        if (configFile == null && !config.hasContent()) {
            throw new ConfigurationArgsParseException("Failed to parse config content. "
                    + "Please, specify config file or provide config content directly.");
        }

        if (configFile == null) {
            return config.toString();
        } else {
            Config result = ConfigFactory.parseFile(configFile);
            if (config.hasContent()) {
                result = result.withFallback(ConfigFactory.parseString(config.toString()));
            }
            return result.resolve().root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
        }
    }
}
