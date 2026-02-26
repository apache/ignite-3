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

package org.apache.ignite.internal;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;import java.nio.file.Path;

/**
 * Represents details about an Ignite node, including its name, working directory,
 * and configuration file path. This class is a singleton and its properties are
 * injected using configuration properties.
 */
@Singleton
public class IgniteNodeDetails {
    private final String nodeName;

    private final Path workDir;

    private final Path configPath;

    /**
     * Constructs an instance of IgniteNodeDetails.
     *
     * @param nodeName    the name of the node, injected from the configuration property "node-name".
     * @param workDir     the working directory of the node, injected from the configuration property "work-dir".
     * @param configPath  the configuration file path for the node, injected from the configuration property "config-path".
     */
    public IgniteNodeDetails(
            @Value("${node-name}") String nodeName,
            @Value("${work-dir}") Path workDir,
            @Value("${config-path}") Path configPath
    ) {
        this.nodeName = nodeName;
        this.workDir = workDir;
        this.configPath = configPath;
    }

    public String nodeName() {
        return nodeName;
    }

    public Path workDir() {
        return workDir;
    }

    public Path configPath() {
        return configPath;
    }
}
