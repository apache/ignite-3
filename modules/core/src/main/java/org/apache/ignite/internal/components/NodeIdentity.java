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

package org.apache.ignite.internal.components;

import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Holds basic node identity and environment parameters that components across all modules commonly need.
 *
 * <p>This POJO is created early during node startup and registered as a singleton bean in the DI context,
 * so any module that depends on {@code ignite-core} can inject it without creating a dependency on the runner module.
 */
public class NodeIdentity {
    private final String nodeName;

    private final Path workDir;

    private final Path configPath;

    private final Supplier<UUID> clusterIdSupplier;

    /** Constructor. */
    public NodeIdentity(String nodeName, Path workDir, Path configPath, Supplier<UUID> clusterIdSupplier) {
        this.nodeName = nodeName;
        this.workDir = workDir;
        this.configPath = configPath;
        this.clusterIdSupplier = clusterIdSupplier;
    }

    /** Node name. */
    public String nodeName() {
        return nodeName;
    }

    /** Node working directory. */
    public Path workDir() {
        return workDir;
    }

    /** Path to the node configuration file. */
    public Path configPath() {
        return configPath;
    }

    /** Supplier of the cluster ID; resolved lazily after cluster initialization. */
    public Supplier<UUID> clusterIdSupplier() {
        return clusterIdSupplier;
    }
}
