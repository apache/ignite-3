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

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point for handling the grid lifecycle.
 */
public interface Ignition {
    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration HOCON file. Can be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return CompletableFuture that resolves to an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(String nodeName, @Nullable Path configPath, Path workDir);

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file, with an optional class loader for further usage by
     * {@link java.util.ServiceLoader}.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration HOCON file. Can be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @param serviceLoaderClassLoader Class loader to be used to load provider-configuration files and provider classes; {@code
     * null} if the system class loader (or, failing that, the bootstrap class loader) is to be used
     * @return CompletableFuture that resolves to an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(
            String nodeName,
            @Nullable Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    );

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a URL pointing a HOCON configuration file.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param cfgUrl URL pointing to the node configuration HOCON file. Can be {@code null}.
     * @param workDir Work directory for started node. Must not be {@code null}.
     * @return CompletableFuture that resolves to an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(String nodeName, @Nullable URL cfgUrl, Path workDir);

    /**
     * Starts an Ignite node with an optional bootstrap configuration from an input stream with a HOCON configuration file.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param config Optional node configuration.
     *      The following rules are used for applying the configuration properties:
     *      <ol>
     *        <li>Specified property overrides the existing one, if any.</li>
     *        <li>All non-specified properties use either the previous value or use default one from
     *            the corresponding configuration schema.</li>
     *      </ol>
     *      Therefore, for the initial node start (first start ever), the specified configuration augmented
     *      with defaults, is used. If no configuration is provided, defaults are used for all
     *      configuration properties. For a node restart, the specified properties override existing
     *      ones, and the non-specified properties that hadn't been specified previously use the default values.
     *      The previously specified property values are retrieved from the user-specified {@code workDir}.
     *
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return CompletableFuture that resolves to an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    //TODO: Move IGNITE-18778
    CompletableFuture<Ignite> start(String nodeName, @Nullable InputStream config, Path workDir);

    /**
     * Starts an Ignite node with the default configuration.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return CompletableFuture that resolves to an Ignite node after all
     *         components are started and the cluster initialization is complete.
     */
    //TODO: Move IGNITE-18778
    CompletableFuture<Ignite> start(String nodeName, Path workDir);

    /**
     * Stops the node identified by {@code name}. It is possible to stop both running nodes and the node that are currently starting.
     * No action is taken if the specified node doesn't exist.
     *
     * @param nodeName Name of the node to stop.
     */
    void stop(String nodeName);

    /**
     * Initializes the cluster that the given node is present in.
     *
     * <p>Cluster initialization propagates information about those nodes that will host the Meta Storage and CMG Raft groups
     * to all nodes in the cluster. After the operation succeeds, nodes can finish the start procedure and begin
     * accepting incoming requests.
     *
     * <p>Meta Storage is responsible for storing cluster-wide meta information required for internal purposes and proper functioning of the
     * cluster.
     *
     * <p>Cluster Management Group (CMG) is a Raft group responsible for managing parts of the cluster lifecycle, such as
     * validating incoming nodes and maintaining logical topology.
     *
     * @param parameters initialization parameters.
     * @throws IgniteException If the given node has not been started or has been stopped.
     * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
     */
    void init(InitParameters parameters);
}
