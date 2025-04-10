/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.config;

import static org.apache.ignite.migrationtools.config.ConfigurationConverter.convertConfigurationFile;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.concurrent.Memoizer;
import org.apache.commons.lang3.function.Failable;
import org.apache.commons.lang3.tuple.Pair;

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

        // Set up properties. One of the caches requires this, should be configurable in the future.
        Map<String, String> oldProps = null;
        try {
            Map<String, String> newProps = Map.of(
                    "GRIDGAIN_SERVER_USERNAME", "user",
                    "GRIDGAIN_SERVER_PASSWORD", "password",
                    "GRIDGAIN_CLIENT_USERNAME", "user",
                    "GRIDGAIN_CLIENT_PASSWORD", "password"
            );

            oldProps = newProps.keySet().stream()
                    .map(key -> Pair.of(key, System.getProperty(key)))
                    .filter(e -> e.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            newProps.forEach(System::setProperty);

            convertConfigurationFile(inputCfgPath.toFile(), nodeCfgPath.toFile(), clusterCfgPath.toFile(), false, null);
        } finally {
            if (oldProps != null) {
                oldProps.forEach(System::setProperty);
            }
        }


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
