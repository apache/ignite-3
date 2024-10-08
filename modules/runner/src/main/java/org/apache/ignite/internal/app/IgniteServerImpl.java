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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.copyExceptionWithCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.restart.IgniteAttachmentLock;
import org.apache.ignite.internal.restart.RestartProofIgnite;
import org.apache.ignite.lang.ClusterInitFailureException;
import org.apache.ignite.lang.ClusterNotInitializedException;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeNotStartedException;
import org.apache.ignite.lang.NodeStartException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of embedded node.
 */
public class IgniteServerImpl implements IgniteServer {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(IgniteServerImpl.class);

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

    private final Executor asyncContinuationExecutor;

    /** Current Ignite instance. */
    private volatile @Nullable IgniteImpl ignite;

    /**
     * Lock used to make sure user operations don't see Ignite instances in detached state (which might occur due to a restart)
     * and that user operations linearize wrt detach/attach pairs (due to restarts).
     */
    private final IgniteAttachmentLock attachmentLock;

    private final Ignite publicIgnite;

    private volatile @Nullable CompletableFuture<Void> joinFuture;

    private final Object igniteChangeMutex = new Object();

    private final Object restartOrShutdownMutex = new Object();

    /**
     * Used to make sure restart and shutdown requests are serviced sequentially.
     *
     * <p>Guarded by {@link #restartOrShutdownMutex}.
     */
    @Nullable
    private CompletableFuture<Void> restartOrShutdownFuture;

    /**
     * Gets set to {@code true} when the node shutdown is initiated. This disallows restarts.
     */
    private volatile boolean shutDown;

    /**
     * Constructs an embedded node.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null}. Must exist
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param classLoader The class loader to be used to load provider-configuration files and provider classes, or {@code null} if
     *         the system class loader (or, failing that, the bootstrap class loader) is to be used
     */
    public IgniteServerImpl(String nodeName, Path configPath, Path workDir, @Nullable ClassLoader classLoader) {
        if (nodeName == null) {
            throw new NodeStartException("Node name must not be null");
        }
        if (nodeName.isEmpty()) {
            throw new NodeStartException("Node name must not be empty.");
        }
        if (configPath == null) {
            throw new NodeStartException("Config path must not be null");
        }
        if (Files.notExists(configPath)) {
            throw new NodeStartException("Config file doesn't exist");
        }
        if (workDir == null) {
            throw new NodeStartException("Working directory must not be null");
        }

        this.nodeName = nodeName;
        this.configPath = configPath;
        this.workDir = workDir;
        this.classLoader = classLoader;

        asyncContinuationExecutor = ForkJoinPool.commonPool();
        attachmentLock = new IgniteAttachmentLock(() -> ignite, asyncContinuationExecutor);
        publicIgnite = new RestartProofIgnite(attachmentLock);
    }

    @Override
    public Ignite api() {
        IgniteImpl instance = ignite;
        if (instance == null) {
            throw new NodeNotStartedException();
        }

        throwIfNotJoined();

        return publicIgnite;
    }

    private void throwIfNotJoined() {
        CompletableFuture<Void> joinFuture = this.joinFuture;
        if (joinFuture == null || !joinFuture.isDone()) {
            throw new ClusterNotInitializedException();
        }
        if (joinFuture.isCancelled()) {
            throw new ClusterInitFailureException("Cluster initialization cancelled.");
        }
        if (joinFuture.isCompletedExceptionally()) {
            throw new ClusterInitFailureException("Cluster initialization failed.", joinFuture.handle((res, ex) -> ex).join());
        }
    }

    @Override
    public CompletableFuture<Void> initClusterAsync(InitParameters parameters) {
        IgniteImpl instance = ignite;
        if (instance == null) {
            throw new NodeNotStartedException();
        }
        try {
            return instance.initClusterAsync(parameters.metaStorageNodeNames(),
                    parameters.cmgNodeNames(),
                    parameters.clusterName(),
                    parameters.clusterConfiguration()
            ).thenCompose(unused -> waitForInitAsync());
        } catch (NodeStoppingException e) {
            throw new ClusterInitFailureException("Node stop detected during init", e);
        }
    }

    @Override
    public void initCluster(InitParameters parameters) {
        sync(initClusterAsync(parameters));
    }

    @Override
    public CompletableFuture<Void> waitForInitAsync() {
        IgniteImpl instance = ignite;
        if (instance == null) {
            throw new NodeNotStartedException();
        }

        CompletableFuture<Void> joinFuture = this.joinFuture;
        if (joinFuture == null) {
            try {
                joinFuture = instance.joinClusterAsync()
                        .handle((ignite, e) -> {
                            if (e == null) {
                                ackSuccessStart();

                                return null;
                            } else {
                                throw handleStartException(e);
                            }
                        });
            } catch (Exception e) {
                throw handleStartException(e);
            }
            this.joinFuture = joinFuture;
        }
        return joinFuture;
    }

    /**
     * Restarts the node asynchronously. The {@link Ignite} instance obtained via {@link #api()} and objects acquired through it remain
     * functional, but completion of calls to them might be delayed during the restart (that is, synchronous calls might take more time,
     * while futures from asynchronous calls might take more time to complete).
     *
     * @return CompletableFuture that gets completed when the node startup has completed (either successfully or with an error).
     */
    CompletableFuture<Void> restartAsync() {
        // We do not allow restarts to happen concurrently with shutdowns.
        CompletableFuture<Void> result;
        synchronized (restartOrShutdownMutex) {
            if (shutDown) {
                throw new NodeNotStartedException();
            }

            IgniteImpl instance = ignite;
            if (instance == null) {
                throw new NodeNotStartedException();
            }

            result = chainRestartOrShutdownAction(() -> doRestartAsync(instance));
        }

        return result;
    }

    /**
     * This MUST be called under synchronization on {@link #restartOrShutdownMutex}.
     */
    private CompletableFuture<Void> chainRestartOrShutdownAction(Supplier<CompletableFuture<Void>> action) {
        CompletableFuture<Void> result = (restartOrShutdownFuture == null ? nullCompletedFuture() : restartOrShutdownFuture)
                .thenCompose(unused -> action.get());

        // Suppress exceptions to make sure following futures can be executed.
        restartOrShutdownFuture = result.handle((res, ex) -> null);

        return result;
    }

    private CompletableFuture<Void> doRestartAsync(IgniteImpl instance) {
        // TODO: IGNITE-23006 - limit the wait to acquire the write lock with a timeout.
        return attachmentLock.detachedAsync(() -> {
            synchronized (igniteChangeMutex) {
                this.ignite = null;
            }
            this.joinFuture = null;

            return instance.stopAsync()
                    .thenCompose(unused -> doStartAsync())
                    .thenCompose(unused -> waitForInitAsync());
        });
    }

    @Override
    public CompletableFuture<Void> shutdownAsync() {
        shutDown = true;

        // We don't use attachmentLock here so that users see NodeStoppingException immediately (instead of pausing their operations
        // forever which would happen if the lock was used).

        CompletableFuture<Void> result;

        synchronized (restartOrShutdownMutex) {
            result = chainRestartOrShutdownAction(this::doShutdownAsync);
        }

        // Trigger stop to avoid situations when a restart future caused a join that cannot complete, so restart never completes,
        // so our shutdown can never happen.
        triggerStopOnCurrentIgnite();

        return result;
    }

    private void triggerStopOnCurrentIgnite() {
        IgniteImpl currentIgnite;
        synchronized (igniteChangeMutex) {
            currentIgnite = ignite;
        }
        if (currentIgnite != null) {
            currentIgnite.stopAsync();
        }
    }

    private CompletableFuture<Void> doShutdownAsync() {
        IgniteImpl instance = ignite;
        if (instance != null) {
            try {
                return instance.stopAsync().thenRun(() -> {
                    synchronized (igniteChangeMutex) {
                        ignite = null;
                    }
                    joinFuture = null;
                });
            } catch (Exception e) {
                throw new IgniteException(Common.NODE_STOPPING_ERR, e);
            }
        }
        return nullCompletedFuture();
    }

    @Override
    public void shutdown() {
        sync(shutdownAsync());
    }

    @Override
    public String name() {
        return nodeName;
    }

    /**
     * Starts ignite node.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @return Future that will be completed when the node is started.
     */
    public CompletableFuture<Void> startAsync() {
        if (ignite != null) {
            throw new NodeStartException("Node is already started.");
        }

        // Doing start in blocked state to avoid an overlap with a restart if a restart request comes when the node is still being started.
        return attachmentLock.detachedAsync(this::doStartAsync);
    }

    private CompletableFuture<Void> doStartAsync() {
        if (shutDown) {
            return failedFuture(new NodeNotStartedException());
        }

        IgniteImpl instance = new IgniteImpl(this, this::restartAsync, configPath, workDir, classLoader, asyncContinuationExecutor);

        ackBanner();

        return instance.startAsync().handle((result, throwable) -> {
            if (throwable != null) {
                return CompletableFuture.<Void>failedFuture(throwable);
            }

            synchronized (igniteChangeMutex) {
                if (shutDown) {
                    return instance.stopAsync();
                }

                ignite = instance;
            }

            return completedFuture(result);
        }).thenCompose(identity());
    }

    /**
     * Starts ignite node.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     */
    public void start() {
        sync(startAsync());
    }

    private static IgniteException handleStartException(Throwable e) {
        if (e instanceof IgniteException) {
            return (IgniteException) e;
        } else {
            return new NodeStartException("Error during node start.", e);
        }
    }

    private static void ackSuccessStart() {
        LOG.info("Apache Ignite started successfully!");
    }

    private static void ackBanner() {
        String banner = String.join(lineSeparator(), BANNER);

        String padding = " ".repeat(22);

        String version;

        try (InputStream versionStream = IgniteServerImpl.class.getClassLoader().getResourceAsStream("ignite.version.full")) {
            if (versionStream != null) {
                version = new String(versionStream.readAllBytes(), StandardCharsets.UTF_8);
            } else {
                version = IgniteProductVersion.CURRENT_VERSION.toString();
            }
        } catch (IOException e) {
            version = IgniteProductVersion.CURRENT_VERSION.toString();
        }

        LOG.info("{}" + lineSeparator() + "{}{}" + lineSeparator(), banner, padding, "Apache Ignite ver. " + version);
    }

    private static void sync(CompletableFuture<Void> future) {
        try {
            future.get();
        } catch (ExecutionException e) {
            throw sneakyThrow(tryToCopyExceptionWithCause(e));
        } catch (InterruptedException e) {
            throw sneakyThrow(e);
        }
    }

    // TODO: remove after IGNITE-22721 gets resolved.
    private static Throwable tryToCopyExceptionWithCause(ExecutionException exception) {
        Throwable copy = copyExceptionWithCause(exception);

        if (copy == null) {
            return new IgniteException(INTERNAL_ERR, "Cannot make a proper copy of " + exception.getCause().getClass(), exception);
        }

        return copy;
    }

    /**
     * Returns the underlying IgniteImpl even if the join was not completed.
     */
    @TestOnly
    public IgniteImpl igniteImpl() {
        IgniteImpl instance = ignite;
        if (instance == null) {
            throw new NodeNotStartedException();
        }

        return instance;
    }

    /**
     * Returns future that gets completed when restart or shutdown is complete.
     */
    @TestOnly
    public @Nullable CompletableFuture<Void> restartOrShutdownFuture() {
        synchronized (restartOrShutdownMutex) {
            return restartOrShutdownFuture;
        }
    }
}
