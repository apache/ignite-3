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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded Ignite node. Manages node's lifecycle, provides Ignite API and allows cluster initialization.
 */
public interface IgniteServer {
    /**
     * Starts an embedded Ignite node with a configuration from a HOCON file.
     *
     * <p>When the future returned from this method completes, the node is partially started, and is ready to accept the init command (that
     * is, its REST endpoint is
     * functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return Future that will be completed when the node is partially started, and is ready to accept the init command (that is, its REST
     *         endpoint is functional).
     */
    static CompletableFuture<IgniteServer> startAsync(String nodeName, Path configPath, Path workDir) {
        return startAsync(nodeName, configPath, workDir, null);
    }

    /**
     * Starts an embedded Ignite node with a configuration from a HOCON file with an optional class loader for further usage by
     * {@link java.util.ServiceLoader}..
     *
     * <p>When the future returned from this method completes, the node is partially started, and is ready to accept the init command (that
     * is, its REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @param serviceLoaderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
     *         {@code null} if the system class loader (or, failing that, the bootstrap class loader) is to be used
     * @return Future that will be completed when the node is partially started, and is ready to accept the init command (that is, its REST
     *         endpoint is functional).
     */
    static CompletableFuture<IgniteServer> startAsync(
            String nodeName,
            Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    ) {
        IgniteServerImpl embeddedNode = new IgniteServerImpl(nodeName, configPath, workDir, serviceLoaderClassLoader);
        return embeddedNode.startAsync().thenApply(unused -> embeddedNode);
    }

    /**
     * Starts an embedded Ignite node with a configuration from a HOCON file with an optional class loader for further usage by
     * {@link java.util.ServiceLoader}..
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its REST endpoint is
     * functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return Node instance.
     */
    static IgniteServer start(String nodeName, Path configPath, Path workDir) {
        return start(nodeName, configPath, workDir, null);
    }

    /**
     * Creates and starts an embedded Ignite node with a configuration from a HOCON file, with an optional class loader for further usage by
     * {@link java.util.ServiceLoader}.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its REST endpoint is
     * functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceLoaderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
     *         {@code null} if the system class loader (or, failing that, the bootstrap class loader) is to be used
     */
    static IgniteServer start(
            String nodeName,
            Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    ) {
        IgniteServerImpl embeddedNode = new IgniteServerImpl(nodeName, configPath, workDir, serviceLoaderClassLoader);
        embeddedNode.start();
        return embeddedNode;
    }

    /**
     * Returns the Ignite API. The API is available when the node has joined an initialized cluster. This method throws a
     * {@link org.apache.ignite.lang.ClusterNotInitializedException} if the cluster is not yet initialized.
     *
     * @return Ignite API facade.
     */
    Ignite api();

    /**
     * Initializes the cluster that the given node is present in.
     *
     * <p>Cluster initialization propagates information about those nodes that will host the Meta Storage and CMG Raft groups
     * to all nodes in the cluster. After the operation succeeds, nodes can finish the start procedure and begin accepting incoming
     * requests.
     *
     * <p>Meta Storage is responsible for storing cluster-wide meta information required for internal purposes and proper functioning of
     * the cluster.
     *
     * <p>Cluster Management Group (CMG) is a Raft group responsible for managing parts of the cluster lifecycle, such as
     * validating incoming nodes and maintaining logical topology.
     *
     * @param parameters initialization parameters.
     * @return CompletableFuture that resolves after all components are started and the cluster initialization is complete and the node has
     *         joined the logical topology.
     * @throws IgniteException If the given node has not been started or has been stopped.
     * @see <a
     *         href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77
     *         </a>
     */
    CompletableFuture<Void> initClusterAsync(InitParameters parameters);

    /**
     * Initializes the cluster that the given node is present in.
     *
     * <p>Cluster initialization propagates information about those nodes that will host the Meta Storage and CMG Raft groups
     * to all nodes in the cluster. After the operation succeeds, nodes can finish the start procedure and begin accepting incoming
     * requests.
     *
     * <p>Meta Storage is responsible for storing cluster-wide meta information required for internal purposes and proper functioning of
     * the cluster.
     *
     * <p>Cluster Management Group (CMG) is a Raft group responsible for managing parts of the cluster lifecycle, such as
     * validating incoming nodes and maintaining logical topology.
     *
     * <p>When this method returns, the node is started, the cluster initialization is complete and the node has joined the logical
     * topology.
     *
     * @param parameters initialization parameters.
     * @throws IgniteException If the given node has not been started or has been stopped.
     * @see <a
     *         href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
     */
    void initCluster(InitParameters parameters);

    /**
     * Waits for the cluster initialization. Used when the initialization is done externally (for example, via REST endpoint).
     *
     * @return CompletableFuture that resolves the cluster initialization is complete and the node has joined the logical topology.
     */
    CompletableFuture<Void> waitForInitAsync();

    /**
     * Stops the node.
     */
    CompletableFuture<Void> shutdownAsync();

    /**
     * Stops the node synchronously. After this method completes, the node
     */
    void shutdown();

    /**
     * Returns node name.
     *
     * @return Node name.
     */
    String name();
}
