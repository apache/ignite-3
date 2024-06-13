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
import org.apache.ignite.internal.app.EmbeddedNodeImpl;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded Ignite node. Manages node's lifecycle, provides Ignite API and allows cluster initialization.
 */
public interface EmbeddedNode {
    /**
     * Creates and starts an embedded Ignite node with a configuration from a HOCON file.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    static EmbeddedNode create(String nodeName, Path configPath, Path workDir) {
        EmbeddedNodeImpl igniteEmbedded = new EmbeddedNodeImpl(nodeName, configPath, workDir);
        igniteEmbedded.start(); // Ignite future is kept inside this instance and will be returned from the joinClusterAsync.
        return igniteEmbedded;
    }

    /**
     * Creates and starts an embedded Ignite node with a configuration from a HOCON file, with an optional class loader for further usage by
     * {@link java.util.ServiceLoader}.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceLoaderClassLoader The class loader to be used to load provider-configuration files and provider classes, or {@code
     * null} if the system class loader (or, failing that, the bootstrap class loader) is to be used
     */
    static EmbeddedNode create(
            String nodeName,
            Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    ) {
        EmbeddedNode embeddedNode = new EmbeddedNodeImpl(nodeName, configPath, workDir, serviceLoaderClassLoader);
        embeddedNode.start(); // Ignite future is kept inside this instance and will be returned from the igniteAsync.
        return embeddedNode;
    }

    /**
     * Returns the Ignite API. The API is available when the node has joined an initialized cluster.
     *
     * @return Ignite API future.
     */
    CompletableFuture<Ignite> igniteAsync();

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
     * @return CompletableFuture that resolves to an Ignite node after all components are started and the cluster initialization is
     *         complete.
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
     * @param parameters initialization parameters.
     * @throws IgniteException If the given node has not been started or has been stopped.
     * @see <a
     *         href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
     */
    void initCluster(InitParameters parameters);

    /**
     * Stops the node.
     */
    CompletableFuture<Void> stopAsync();

    /**
     * Stops the node synchronously.
     */
    void stop();

    /**
     * Starts the node. When this method returns, the node is partially started, and is ready to accept the init command (that is, its REST
     * endpoint is functional).
     */
    void start();

    /**
     * Returns node name.
     *
     * @return Node name.
     */
    String name();
}
