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
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded Ignite node. Manages node's lifecycle, provides Ignite API and allows cluster initialization.
 *
 * <p>NOTE: Methods of this interface are not thread-safe and shouldn't be called from different threads.
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
        IgniteServer server = builder(nodeName, configPath, workDir).build();
        return server.startAsync().thenApply(unused -> server);
    }

    /**
     * Starts an embedded Ignite node with a configuration from a HOCON string.
     *
     * <p>When the future returned from this method completes, the node is partially started, and is ready to accept the init command (that
     * is, its REST endpoint is
     * functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configString Node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return Future that will be completed when the node is partially started, and is ready to accept the init command (that is, its REST
     *         endpoint is functional).
     */
    static CompletableFuture<IgniteServer> startAsync(String nodeName, String configString, Path workDir) {
        IgniteServer server = builder(nodeName, configString, workDir).build();
        return server.startAsync().thenApply(unused -> server);
    }

    /**
     * Starts the node.
     *
     * <p>When the returned future completes, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * <p>Start can only be called once.
     *
     * @return Future that will be completed when the node is started.
     */
    CompletableFuture<Void> startAsync();

    /**
     * Starts an embedded Ignite node with a configuration from a HOCON file synchronously.
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
        IgniteServer server = builder(nodeName, configPath, workDir).build();
        server.start();
        return server;
    }

    /**
     * Starts an embedded Ignite node with a configuration from a HOCON string synchronously.
     *
     * <p>When this method returns, the node is partially started, and is ready to accept the init command (that is, its REST endpoint is
     * functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configString Node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return Node instance.
     */
    static IgniteServer start(String nodeName, String configString, Path workDir) {
        IgniteServer server = builder(nodeName, configString, workDir).build();
        server.start();
        return server;
    }

    /**
     * Starts the node.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * <p>Start can only be called once.
     */
    void start();

    /**
     * Returns a builder for an embedded Ignite node.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return Node instance.
     */
    static Builder builder(String nodeName, Path configPath, Path workDir) {
        return new Builder(nodeName, configPath, workDir);
    }

    /**
     * Returns a builder for an embedded Ignite node.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configString Node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the node. Must not be {@code null}.
     * @return Node instance.
     */
    static Builder builder(String nodeName, String configString, Path workDir) {
        return new Builder(nodeName, configString, workDir);
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
     * Initializes the cluster that the given node is present in synchronously.
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
     * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-77%3A+Node+Join+Protocol+and+Initialization+for+Ignite+3">IEP-77</a>
     */
    void initCluster(InitParameters parameters);

    /**
     * Waits for the cluster initialization. Used when the initialization is done externally (for example, via REST endpoint).
     *
     * @return CompletableFuture that resolves when the cluster initialization is complete and the node has joined the logical topology.
     */
    CompletableFuture<Void> waitForInitAsync();

    /**
     * Stops the node. After the future returned by method completes, the node is no longer functional and can't be restarted. Use
     * {@code startAsync} methods to create new {@code IgniteServer} instance.
     *
     * @return CompletableFuture that resolves when the node is stopped.
     */
    CompletableFuture<Void> shutdownAsync();

    /**
     * Stops the node synchronously. After this method completes, the node is no longer functional and can't be restarted. Use
     * {@code startAsync} methods to create new {@code IgniteServer} instance.
     */
    void shutdown();

    /**
     * Returns node name.
     *
     * @return Node name.
     */
    String name();

    /**
     * Builder for IgniteServer.
     */
    final class Builder {
        private final String nodeName;
        private final Path workDir;

        private Path configPath;
        private String configString;

        private @Nullable ClassLoader serviceLoaderClassLoader;
        private Executor asyncContinuationExecutor = ForkJoinPool.commonPool();

        private Builder(String nodeName, Path configPath, Path workDir) {
            this.nodeName = nodeName;
            this.configPath = configPath;
            this.workDir = workDir;
        }

        private Builder(String nodeName, String configString, Path workDir) {
            this.nodeName = nodeName;
            this.configString = configString;
            this.workDir = workDir;
        }

        /**
         * Specifies class loader to use when loading components via {@link java.util.ServiceLoader}.
         *
         * @param serviceLoaderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
         *         {@code null} if the system class loader (or, failing that, the bootstrap class loader) is to be used
         * @return This instance for chaining.
         */
        public Builder serviceLoaderClassLoader(@Nullable ClassLoader serviceLoaderClassLoader) {
            this.serviceLoaderClassLoader = serviceLoaderClassLoader;
            return this;
        }

        /**
         * Specifies executor in which futures obtained via API will be completed..
         *
         * @param asyncContinuationExecutor Executor in which futures obtained via API will be completed.
         * @return This instance for chaining.
         */
        public Builder asyncContinuationExecutor(Executor asyncContinuationExecutor) {
            this.asyncContinuationExecutor = asyncContinuationExecutor;
            return this;
        }

        /**
         * Builds an IgniteServer. It is not started; {@link #start()} or {@link #startAsync()} can be used to start it.
         *
         * @return Server instance.
         */
        public IgniteServer build() {
            return new IgniteServerImpl(
                    nodeName,
                    configPath,
                    configString,
                    workDir,
                    serviceLoaderClassLoader,
                    asyncContinuationExecutor
            );
        }
    }
}
