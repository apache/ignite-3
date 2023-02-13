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

package org.apache.ignite.internal.configuration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

/**
 * Node bootstrap configuration provider interface.
 */
@FunctionalInterface
public interface NodeBootstrapConfiguration {
    /**
     * Default name of configuration file.
     */
    String DEFAULT_CONFIG_NAME = "ignite-config.conf";

    /**
     * Path to node configuration file.
     *
     * @return Path to node configuration file in HOCON format.
     */
    Path configPath();

    /**
     * Simple config file provider.
     *
     * @param configPath Path to node bootstrap configuration.
     * @return Simple implementation with provided configuration file.
     */
    static NodeBootstrapConfiguration directFile(Path configPath) {
        return () -> configPath;
    }

    /**
     * Return node bootstrap configuration with content from {@param is}.
     *
     * @param is Configuration content.
     * @param workDir Dir for configuration file location.
     * @return Node bootstrap configuration with lazy config file creation.
     */
    //TODO: Move IGNITE-18778
    static NodeBootstrapConfiguration inputStream(@Nullable InputStream is, Path workDir) {
        return new NodeBootstrapConfiguration() {

            private final AtomicReference<Path> config = new AtomicReference<>();

            @Override
            public Path configPath() {
                if (config.compareAndSet(null, createEmptyConfig(workDir))) {
                    if (is != null) {
                        byte[] bytes;
                        try {
                            bytes = is.readAllBytes();
                        } catch (IOException e) {
                            throw new NodeConfigReadException("Failed to read config input stream.", e);
                        }
                        try {
                            Files.write(config.get(), bytes, StandardOpenOption.SYNC);
                        } catch (IOException e) {
                            throw new NodeConfigWriteException("Failed to write config content to file.", e);
                        }
                    }
                }

                return config.get();
            }
        };
    }

    /**
     * Return node bootstrap configuration with content from {@param plainConf}.
     *
     * @param plainConf Configuration content.
     * @param workDir Dir for configuration file location.
     * @return Node bootstrap configuration with lazy config file creation.
     */
    //TODO: Move IGNITE-18778
    static NodeBootstrapConfiguration string(@Nullable @Language("HOCON") String plainConf, Path workDir) {
        InputStream is = plainConf != null
                ? new ByteArrayInputStream(plainConf.getBytes(StandardCharsets.UTF_8))
                : null;
        return inputStream(is, workDir);
    }

    /**
     * Empty config provider.
     *
     * @param workDir Configuration file location.
     * @return Node bootstrap configuration provider to empty config.
     */
    static NodeBootstrapConfiguration empty(Path workDir) {
        return () -> createEmptyConfig(workDir);
    }

    private static Path createEmptyConfig(Path workDir) {
        try {
            Path config = workDir.resolve(DEFAULT_CONFIG_NAME);
            File file = config.toFile();

            if (!file.exists()) {
                file.createNewFile();
            }

            return config;
        } catch (IOException e) {
            throw new NodeConfigCreateException("Failed to create conf file.", e);
        }
    }
}
