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

package org.apache.ignite;

import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Service loader-based implementation of an entry point for handling the grid lifecycle.
 */
public class IgnitionManager {
    /**
     * Loaded Ignition instance.
     *
     * <p>Concurrent access is guarded by the {@link Class} instance.
     */
    @Nullable
    private static Ignition ignition;

    /**
     * Starts an Ignite node with a bootstrap configuration from an input stream with a HOCON configuration file.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param cfgPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    public static CompletableFuture<Ignite> start(String nodeName, Path cfgPath, Path workDir) {
        return start(nodeName, cfgPath, workDir, null);
    }

    /**
     * Starts an Ignite node with a bootstrap configuration from a HOCON file.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param cfgPath  Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir  Work directory for the started node. Must not be {@code null}.
     * @param clsLdr   The class loader to be used to load provider-configuration files and provider classes, or {@code null} if the system
     *                 class loader (or, failing that, the bootstrap class loader) is to be used
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    // TODO IGNITE-14580 Add exception handling logic to IgnitionProcessor.
    public static CompletableFuture<Ignite> start(String nodeName, Path cfgPath, Path workDir, @Nullable ClassLoader clsLdr) {
        Ignition ignition = loadIgnitionService(clsLdr);

        return ignition.start(nodeName, cfgPath, workDir, clsLdr);
    }

    /**
     * Stops the node identified by {@code nodeName}. It is possible to stop both running nodes and the node that are currently starting.
     * No action is taken if the specified node doesn't exist.
     *
     * @param nodeName Name of the node to stop.
     */
    public static void stop(String nodeName) {
        stop(nodeName, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Stops the node identified by {@code nodeName}. It is possible to stop both running nodes and the node that are currently starting.
     * No action is taken if the specified node doesn't exist.
     *
     * @param nodeName Name of the node to stop.
     * @param clsLdr Class loader to be used to load provider-configuration files and provider classes; {@code null} if the system
     *               class loader (or, failing that, the bootstrap class loader) is to be used.
     */
    public static void stop(String nodeName, @Nullable ClassLoader clsLdr) {
        Ignition ignition = loadIgnitionService(clsLdr);

        ignition.stop(nodeName);
    }

    /**
     * Initializes the cluster the specified node belongs to.
     *
     * @param nodeName Name of the node that the initialization request will be sent to.
     * @param parameters initialization parameters.
     * @throws IgniteException If the given node has not been started or has been stopped.
     * @throws NullPointerException If any of the parameters are null.
     * @throws IllegalArgumentException If {@code metaStorageNodeNames} is empty or contains blank strings.
     * @throws IllegalArgumentException If {@code cmgNodeNames} contains blank strings.
     * @throws IllegalArgumentException If {@code clusterName} is blank.
     * @see Ignition#init(String, InitParameters)
     */
    public static synchronized void init(String nodeName, InitParameters parameters) {
        if (ignition == null) {
            throw new IgniteException("Ignition service has not been started");
        }

        ignition.init(nodeName, parameters);
    }

    private static synchronized Ignition loadIgnitionService(@Nullable ClassLoader clsLdr) {
        if (ignition == null) {
            ServiceLoader<Ignition> ldr = ServiceLoader.load(Ignition.class, clsLdr);
            ignition = ldr.iterator().next();
        }

        return ignition;
    }
}
