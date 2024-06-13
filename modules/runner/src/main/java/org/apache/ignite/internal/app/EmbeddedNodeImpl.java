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

package org.apache.ignite.internal.app;

import static java.lang.System.lineSeparator;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.EmbeddedNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of embedded node.
 */
public class EmbeddedNodeImpl implements EmbeddedNode {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(EmbeddedNodeImpl.class);

    private static final String[] BANNER = {
            "",
            "           #              ___                         __",
            "         ###             /   |   ____   ____ _ _____ / /_   ___",
            "     #  #####           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
            "   ###  ######         / ___ | / /_/ // /_/ // /__ / / / // ___/",
            "  #####  #######      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
            "  #######  ######            /_/",
            "    ########  ####        ____               _  __           _____",
            "   #  ########  ##       /  _/____ _ ____   (_)/ /_ ___     |__  /",
            "  ####  #######  #       / / / __ `// __ \\ / // __// _ \\     /_ <",
            "   #####  #####        _/ / / /_/ // / / // // /_ / ___/   ___/ /",
            "     ####  ##         /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
            "       ##                  /____/\n"
    };

    static {
        ErrorGroups.initialize();
        IgniteEventType.initialize();
    }

    private final String nodeName;

    private final Path configPath;

    private final Path workDir;

    private final ClassLoader classLoader;

    private volatile @Nullable IgniteImpl instance;

    private volatile @Nullable CompletableFuture<Ignite> igniteFuture;

    /**
     * Constructs an embedded node.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}. Must exist
     * @param workDir Work directory for the started node. Must not be {@code null}.
     */
    public EmbeddedNodeImpl(String nodeName, Path configPath, Path workDir) {
        this(nodeName, configPath, workDir, defaultServiceClassLoader());
    }

    /**
     * Constructs an embedded node.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}. Must exist
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param classLoader The class loader to be used to load provider-configuration files and provider classes, or {@code null} if
     *         the system class loader (or, failing that, the bootstrap class loader) is to be used
     */
    public EmbeddedNodeImpl(String nodeName, Path configPath, Path workDir, ClassLoader classLoader) {
        if (nodeName == null) {
            throw new IgniteException("Node name must not be null");
        }
        if (nodeName.isEmpty()) {
            throw new IgniteException("Node name must not be empty.");
        }
        if (configPath == null) {
            throw new IgniteException("Config path must not be null");
        }
        if (Files.notExists(configPath)) {
            throw new IgniteException("Config file doesn't exist");
        }
        if (workDir == null) {
            throw new IgniteException("Working directory must not be null");
        }

        this.nodeName = nodeName;
        this.configPath = configPath;
        this.workDir = workDir;
        this.classLoader = classLoader;
    }

    @Override
    public CompletableFuture<Ignite> igniteAsync() {
        IgniteImpl instance = this.instance;
        if (instance == null) {
            throw new IgniteException("Node not started");
        }

        // We need to cache the future so that this method could be called multiple times.
        CompletableFuture<Ignite> igniteFuture = this.igniteFuture;
        if (igniteFuture == null) {
            igniteFuture = instance.joinClusterAsync()
                    .handle((ignite, e) -> {
                        if (e == null) {
                            ackSuccessStart();

                            return ignite;
                        } else {
                            throw handleStartException(e);
                        }
                    });
            this.igniteFuture = igniteFuture;
        }
        return igniteFuture;
    }

    @Override
    public CompletableFuture<Void> initClusterAsync(InitParameters parameters) {
        IgniteImpl instance = this.instance;
        if (instance == null) {
            throw new IgniteException("Node not started");
        }
        try {
            return instance.initClusterAsync(parameters.metaStorageNodeNames(),
                    parameters.cmgNodeNames(),
                    parameters.clusterName(),
                    parameters.clusterConfiguration()
            );
        } catch (NodeStoppingException e) {
            throw new IgniteException("Node stop detected during init", e);
        }
    }

    @Override
    public void initCluster(InitParameters parameters) {
        sync(initClusterAsync(parameters));
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        IgniteImpl instance = this.instance;
        if (instance != null) {
            try {
                return instance.stopAsync().thenRun(() -> {
                    this.instance = null;
                    this.igniteFuture = null;
                });
            } catch (Exception e) {
                throw new IgniteException(Common.NODE_STOPPING_ERR, e);
            }
        }
        return nullCompletedFuture();
    }

    @Override
    public void stop() {
        sync(stopAsync());
    }

    private static ClassLoader defaultServiceClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    @Override
    public void start() {
        if (this.instance != null) {
            throw new IgniteException("Node is already started.");
        }
        IgniteImpl instance = new IgniteImpl(this, configPath, workDir, classLoader);

        ackBanner();

        try {
            instance.start();
        } catch (Exception e) {
            throw handleStartException(e);
        }
        this.instance = instance;
    }

    private static IgniteException handleStartException(Throwable e) {
        if (e instanceof IgniteException) {
            return (IgniteException) e;
        } else {
            return new IgniteException(e);
        }
    }

    private static void ackSuccessStart() {
        LOG.info("Apache Ignite started successfully!");
    }

    private static void ackBanner() {
        String banner = String.join(lineSeparator(), BANNER);

        String padding = " ".repeat(22);

        String version = "Apache Ignite ver. " + IgniteProductVersion.CURRENT_VERSION;

        LOG.info("{}" + lineSeparator() + "{}{}" + lineSeparator(), banner, padding, version);
    }

    @Override
    public String name() {
        return nodeName;
    }

    private static void sync(CompletableFuture<Void> future) {
        try {
            future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(unwrapCause(e));
        }
    }
}
