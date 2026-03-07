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

package org.apache.ignite.internal.app.di;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.internal.disaster.system.ServerRestarter;
import org.jetbrains.annotations.Nullable;

/**
 * Holds the seed parameters provided to an Ignite node at construction time.
 * Registered as a singleton bean in the core DI context so that factories and components
 * can inject it to access node identity, paths, and bootstrap objects.
 */
public class NodeSeedParams {
    private final IgniteServer node;

    private final ServerRestarter restarter;

    private final Path configPath;

    private final Path workDir;

    @Nullable
    private final ClassLoader serviceProviderClassLoader;

    private final Executor asyncContinuationExecutor;

    private final Supplier<UUID> clusterIdSupplier;

    /** Constructor. */
    public NodeSeedParams(
            IgniteServer node,
            ServerRestarter restarter,
            Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceProviderClassLoader,
            Executor asyncContinuationExecutor,
            Supplier<UUID> clusterIdSupplier
    ) {
        this.node = node;
        this.restarter = restarter;
        this.configPath = configPath;
        this.workDir = workDir;
        this.serviceProviderClassLoader = serviceProviderClassLoader;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
        this.clusterIdSupplier = clusterIdSupplier;
    }

    /** Node name. */
    public String nodeName() {
        return node.name();
    }

    /** The Ignite server instance. */
    public IgniteServer node() {
        return node;
    }

    /** Server restarter for handling node restarts. */
    public ServerRestarter restarter() {
        return restarter;
    }

    /** Path to the node configuration file. */
    public Path configPath() {
        return configPath;
    }

    /** Node working directory. */
    public Path workDir() {
        return workDir;
    }

    /** Class loader used to discover service providers, or {@code null} for the system class loader. */
    @Nullable
    public ClassLoader serviceProviderClassLoader() {
        return serviceProviderClassLoader;
    }

    /** Executor in which user-facing futures will be completed. */
    public Executor asyncContinuationExecutor() {
        return asyncContinuationExecutor;
    }

    /** Supplier of the cluster ID; resolved lazily after cluster initialization. */
    public Supplier<UUID> clusterIdSupplier() {
        return clusterIdSupplier;
    }
}
