/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.Collection;
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
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Can be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(String nodeName, @Nullable Path configPath, Path workDir);

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file, with an optional class loader for further usage by
     * {@link java.util.ServiceLoader}.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Can be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceLoaderClassLoader The class loader to be used to load provider-configuration files and provider classes, or {@code
     * null} if the system class loader (or, failing that, the bootstrap class loader) is to be used
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(
            String nodeName,
            @Nullable Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    );

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a URL linking to HOCON configs.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param cfgUrl URL linking to the node configuration in the HOCON format. Can be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(String nodeName, @Nullable URL cfgUrl, Path workDir);

    /**
     * Starts an Ignite node with an optional bootstrap configuration from an input stream with HOCON configs.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param config Optional node configuration based on
     *      {@link org.apache.ignite.configuration.schemas.network.NetworkConfigurationSchema}.
     *      Following rules are used for applying the configuration properties:
     *      <ol>
     *        <li>Specified property overrides existing one or just applies itself if it wasn't
     *            previously specified.</li>
     *        <li>All non-specified properties either use previous value or use default one from
     *            corresponding configuration schema.</li>
     *      </ol>
     *      So that, in case of initial node start (first start ever) specified configuration, supplemented
     *      with defaults, is used. If no configuration was provided defaults are used for all
     *      configuration properties. In case of node restart, specified properties override existing
     *      ones, non specified properties that also weren't specified previously use default values.
     *      Please pay attention that previously specified properties are searched in the
     *      {@code workDir} specified by the user.
     *
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(String nodeName, @Nullable InputStream config, Path workDir);

    /**
     * Starts an Ignite node with the default configuration.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    CompletableFuture<Ignite> start(String nodeName, Path workDir);

    /**
     * Stops the node with given {@code name}. It's possible to stop both already started node or node that is currently starting. Has no
     * effect if node with specified name doesn't exist.
     *
     * @param nodeName Node name to stop.
     */
    void stop(String nodeName);

    /**
     * Initializes the cluster that the given node is present in.
     *
     * <p>Initializing a cluster implies propagating information about the nodes that will host the Meta Storage and CMG Raft groups
     * to all nodes in the cluster. After the operation succeeds, nodes will be able to finish the start procedure and begin
     * accepting incoming requests.
     *
     * <p>Meta Storage is responsible for storing cluster-wide meta information needed for internal purposes and proper functioning of the
     * cluster.
     *
     * <p>Cluster Management Group (a.k.a. CMG) is a Raft group responsible for managing parts of the cluster lifecycle, such as
     * validating incoming nodes and maintaining the logical topology.
     *
     * @param nodeName Name of the node that the initialization request will be sent to.
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage <b>and</b> the CMG.
     * @param clusterName Human-readable name of the cluster.
     * @throws IgniteException If the given node has not been started or has been stopped.
     * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
     */
    void init(String nodeName, Collection<String> metaStorageNodeNames, String clusterName);

    /**
     * Initializes the cluster that the given node is present in.
     *
     * <p>Initializing a cluster implies propagating information about the nodes that will host the Meta Storage and CMG Raft groups
     * to all nodes in the cluster. After the operation succeeds, nodes will be able to finish the start procedure and begin
     * accepting incoming requests.
     *
     * <p>Meta Storage is responsible for storing cluster-wide meta information needed for internal purposes and proper functioning of the
     * cluster.
     *
     * <p>Cluster Management Group (a.k.a. CMG) is a Raft group responsible for managing parts of the cluster lifecycle, such as
     * validating incoming nodes and maintaining the logical topology.
     *
     * @param nodeName Name of the node that the initialization request will be sent to.
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the CMG.
     * @param clusterName Human-readable name of the cluster.
     * @throws IgniteException If the given node has not been started or has been stopped.
     * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
     */
    void init(String nodeName, Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames, String clusterName);
}
