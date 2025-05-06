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

package org.apache.ignite.migrationtools.config;

import static org.apache.ignite.migrationtools.config.ConfigurationConverter.convertConfigurationFile;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.concurrent.Memoizer;
import org.apache.commons.lang3.function.Failable;

/** ConfigTestUtils. */
public class ConfigTestUtils {
    private static final Path resourcesPath = Path.of("../../resources");

    private static Memoizer<String, ConfigTestDescriptor> cachedDescriptors =
            new Memoizer<>(Failable.asFunction(ConfigTestUtils::doLoadResourceFile), false);

    private ConfigTestUtils() {
        // Intentionally left blank
    }

    private static ConfigTestDescriptor doLoadResourceFile(String inputPath) throws Exception {
        Path inputCfgPath = resourcesPath.resolve(inputPath);
        Path nodeCfgPath = Files.createTempFile("node", ".cfg");
        Path clusterCfgPath = Files.createTempFile("cluster", ".cfg");

        convertConfigurationFile(inputCfgPath.toFile(), nodeCfgPath.toFile(), clusterCfgPath.toFile(), false, null);

        String nodeCfgStr = Files.readString(nodeCfgPath, StandardCharsets.UTF_8);
        String clusterCfgStr = Files.readString(clusterCfgPath, StandardCharsets.UTF_8);

        Config nodeCfg = ConfigFactory.parseString(nodeCfgStr);
        Config clusterCfg = ConfigFactory.parseString(clusterCfgStr);

        return new ConfigTestDescriptor(
                inputCfgPath,
                nodeCfgPath,
                clusterCfgPath,
                nodeCfg,
                clusterCfg
        );
    }

    public static ConfigTestDescriptor loadResourceFile(String inputPath) throws Exception {
        return cachedDescriptors.compute(inputPath);
    }

    /** ConfigTestDescriptor. */
    public static class ConfigTestDescriptor {
        // AI Config File
        private final Path sourceFile;

        private final Path nodeCfgPath;

        private final Path clusterCfgPath;

        private final Config nodeCfg;

        private final Config clusterCfg;

        /** Constructor. */
        public ConfigTestDescriptor(Path sourceFile, Path nodeCfgPath, Path clusterCfgPath, Config nodeCfg, Config clusterCfg) {
            this.sourceFile = sourceFile;
            this.nodeCfgPath = nodeCfgPath;
            this.clusterCfgPath = clusterCfgPath;
            this.nodeCfg = nodeCfg;
            this.clusterCfg = clusterCfg;
        }

        public Path getSourceFile() {
            return sourceFile;
        }

        public Path getNodeCfgPath() {
            return nodeCfgPath;
        }

        public Path getClusterCfgPath() {
            return clusterCfgPath;
        }

        public Config getNodeCfg() {
            return nodeCfg;
        }

        public Config getClusterCfg() {
            return clusterCfg;
        }
    }
}
