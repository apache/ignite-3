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
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.copyExceptionWithCause;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryManagerMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
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

    private static final Random RANDOM = new Random();

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
     * Future of the last internal restart future ({@code null} if no internal restarts where made).
     *
     * <p>Guarded by {@link #restartOrShutdownMutex}.
     */
    @Nullable
    private CompletableFuture<Void> restartFuture;

    /**
     * Gets set to {@code true} when the node shutdown is initiated. This disallows restarts.
     */
    private volatile boolean shutDown;

    /**
     * Constructs an embedded node.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Must not be {@code null} if the {@code configString} is
     *         {@code null}. Must exist if not {@code null}.
     * @param configString Node configuration in the HOCON format. Must not be {@code null} if the {@code configPath} is
     *         {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param classLoader The class loader to be used to load provider-configuration files and provider classes, or {@code null} if
     *         the system class loader (or, failing that, the bootstrap class loader) is to be used
     * @param asyncContinuationExecutor Executor in which user-facing futures will be completed.
     */
    public IgniteServerImpl(
            String nodeName,
            @Nullable Path configPath,
            @Nullable String configString,
            Path workDir,
            @Nullable ClassLoader classLoader,
            Executor asyncContinuationExecutor
    ) {
        if (nodeName == null) {
            throw new NodeStartException("Node name must not be null");
        }
        if (nodeName.isEmpty()) {
            throw new NodeStartException("Node name must not be empty.");
        }
        if (configPath == null && configString == null) {
            throw new NodeStartException("Either config path or config string must not be null");
        }
        if (configPath != null && Files.notExists(configPath)) {
            throw new NodeStartException("Config file doesn't exist");
        }
        if (workDir == null) {
            throw new NodeStartException("Working directory must not be null");
        }
        if (asyncContinuationExecutor == null) {
            throw new NodeStartException("Async continuation executor must not be null");
        }

        this.nodeName = nodeName;
        this.configPath = configPath != null ? configPath : createConfigFile(configString, workDir);
        this.workDir = workDir;
        this.classLoader = classLoader;
        this.asyncContinuationExecutor = asyncContinuationExecutor;

        attachmentLock = new IgniteAttachmentLock(() -> ignite, asyncContinuationExecutor);
        publicIgnite = new RestartProofIgnite(attachmentLock);
    }

    private static Path createConfigFile(String config, Path workDir) {
        try {
            Files.createDirectories(workDir);
            Path configFile = Files.writeString(getConfigFile(workDir), config);
            if (!configFile.toFile().setReadOnly()) {
                throw new NodeStartException("Cannot set read-only flag on node configuration file");
            }
            return configFile;
        } catch (IOException e) {
            throw new NodeStartException("Cannot write node configuration file", e);
        }
    }

    private static Path getConfigFile(Path workDir) {
        // First try the well-known name
        Path path = workDir.resolve("ignite-config.conf");
        if (Files.notExists(path)) {
            return path;
        }

        while (Files.exists(path)) {
            path = workDir.resolve("ignite-config." + RANDOM.nextInt() + ".conf");
        }
        return path;
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
            return instance.initClusterAsync(
                    parameters.metaStorageNodeNames(),
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
        CompletableFuture<Void> joinFuture = this.joinFuture;
        if (joinFuture == null) {
            throw new NodeNotStartedException();
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
    public CompletableFuture<Void> restartAsync() {
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

            restartFuture = result;
        }

        return result;
    }

    /**
     * This MUST be called under synchronization on {@link #restartOrShutdownMutex}.
     */
    private CompletableFuture<Void> chainRestartOrShutdownAction(Supplier<CompletableFuture<Void>> action) {
        CompletableFuture<Void> result = (restartOrShutdownFuture == null ? nullCompletedFuture() : restartOrShutdownFuture)
                // Suppress exceptions to make sure previous errors will not affect this step.
                .handle((res, ex) -> null)
                .thenCompose(unused -> action.get());

        restartOrShutdownFuture = result;

        return result;
    }

    private CompletableFuture<Void> doRestartAsync(IgniteImpl instance) {
        // TODO: IGNITE-23006 - limit the wait to acquire the write lock with a timeout.
        return attachmentLock.detachedAsync(() -> {
            synchronized (igniteChangeMutex) {
                LOG.info("Setting Ignite ref to null as restart is initiated [name={}]", nodeName);
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
        // We don't use attachmentLock here so that users see NodeStoppingException immediately (instead of pausing their operations
        // forever which would happen if the lock was used).

        CompletableFuture<Void> result;

        synchronized (restartOrShutdownMutex) {
            if (shutDown) {
                // Someone has already invoked shutdown, so #restartOrShutdownFuture is about shutdown, let's simply return it.
                return requireNonNull(restartOrShutdownFuture);
            }

            shutDown = true;

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
                        LOG.info("Setting Ignite ref to null as shutdown is completed [name={}]", nodeName);
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

    @Override
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

        logAvailableResources();

        logOsInfo();

        logVmInfo();

        logJvmArguments();

        logAvailableVmMemory();

        logGcConfiguration();

        ackRemoteManagement();

        return instance.startAsync().thenCompose(unused -> {
            synchronized (igniteChangeMutex) {
                if (shutDown) {
                    LOG.info("A new Ignite instance has started, but a shutdown is requested, so not setting it, stopping it instead "
                            + "[name={}]", nodeName);

                    return instance.stopAsync();
                }

                LOG.info("Initiating join process [name={}]", nodeName);
                doWaitForInitAsync(instance);

                LOG.info("Setting Ignite ref to new instance as it has started [name={}]", nodeName);
                ignite = instance;
            }

            return nullCompletedFuture();
        });
    }

    private void doWaitForInitAsync(IgniteImpl instance) {
        try {
            joinFuture = instance.joinClusterAsync()
                    .handle((ignite, e) -> {
                        if (e == null) {
                            ackSuccessStart();

                            return null;
                        } else {
                            throw handleClusterStartException(e);
                        }
                    });
        } catch (Exception e) {
            throw handleClusterStartException(e);
        }
    }

    private static IgniteException handleClusterStartException(Throwable e) {
        if (e instanceof IgniteException) {
            return (IgniteException) e;
        } else {
            return new NodeStartException("Error during node start.", e);
        }
    }

    @Override
    public void start() {
        CompletableFuture<Void> startFuture = startAsync().handle(IgniteServerImpl::handleNodeStartException);

        sync(startFuture);
    }

    private static Void handleNodeStartException(Void v, Throwable e) {
        if (e == null) {
            return v;
        }

        throwIfError(e);

        sneakyThrow(e);

        return v;
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

    private static void logAvailableResources() {
        LOG.info("Available processors: {}", Runtime.getRuntime().availableProcessors());
    }

    private static void logOsInfo() {
        Long jvmPid = null;

        try {
            jvmPid = ProcessHandle.current().pid();
        } catch (Throwable ignore) {
            // No-op.
        }

        String osName = System.getProperty("os.name");
        String osVersion = System.getProperty("os.version");
        String osArch = System.getProperty("os.arch");
        String osUser = System.getProperty("user.name");

        LOG.info(
                "OS: [name={}, version={}, arch={}, user={}, pid={}]",
                osName, osVersion, osArch, osUser, (jvmPid == null ? "N/A" : jvmPid)
        );
    }

    private static void logVmInfo() {
        String jreName = System.getProperty("java.runtime.name");
        String jreVersion = System.getProperty("java.runtime.version");
        String jvmVendor = System.getProperty("java.vm.vendor");
        String jvmName = System.getProperty("java.vm.name");
        String jvmVersion = System.getProperty("java.vm.version");

        LOG.info(
                "VM: [jreName={}, jreVersion={}, jvmVendor={}, jvmName={}, jvmVersion={}]",
                jreName, jreVersion, jvmVendor, jvmName, jvmVersion
        );
    }

    private static void logAvailableVmMemory() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemoryUsage = memoryBean.getNonHeapMemoryUsage();

        LOG.info(
                "VM Memory Configuration: heap [init={}, max={}], non-heap [init={}, max={}]",
                toHumanReadableMemoryString(heapMemoryUsage.getInit()),
                toHumanReadableMemoryString(heapMemoryUsage.getMax()),
                toHumanReadableMemoryString(nonHeapMemoryUsage.getInit()),
                toHumanReadableMemoryString(nonHeapMemoryUsage.getMax())
        );
    }

    private static void logJvmArguments() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> jvmArgs = runtimeMxBean.getInputArguments();

        LOG.info("VM arguments: {}", String.join(" ", jvmArgs));
    }

    private static String toHumanReadableMemoryString(long bytes) {
        String[] suffix = { "KB", "MB", "GB", "TB" };
        for (int i = suffix.length; i > 0; i--) {
            int shift = i * 10;
            long valueInUnits = bytes >> shift;
            if (valueInUnits > 0) {
                return valueInUnits + " " + suffix[i - 1];
            }
        }
        return bytes + " B";
    }

    private static void logGcConfiguration() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        String gcConfiguration = gcBeans.stream()
                .map(MemoryManagerMXBean::getName)
                .collect(Collectors.joining(", "));

        LOG.info("VM GC: {}", gcConfiguration);
    }

    private static void ackRemoteManagement() {
        if (LOG.isInfoEnabled()) {
            boolean jmxEnabled = System.getProperty("com.sun.management.jmxremote") != null;

            if (jmxEnabled) {
                String jmxMessage = "Remote management[JMX (remote: on, port: {}, auth: {}, ssl: {})]";

                String port = System.getProperty("com.sun.management.jmxremote.port", "<n/a>");
                boolean authEnabled = Boolean.getBoolean("com.sun.management.jmxremote.authenticate");
                // By default SSL is enabled, that's why additional check for null is needed.
                // https://docs.oracle.com/en/java/javase/11/management/monitoring-and-management-using-jmx-technology.html
                boolean sslEnabled = Boolean.getBoolean("com.sun.management.jmxremote.ssl")
                        || (System.getProperty("com.sun.management.jmxremote.ssl") == null);

                LOG.info(IgniteStringFormatter.format(jmxMessage, port, onOff(authEnabled), onOff(sslEnabled)));
            } else {
                LOG.info("Remote management[JMX (remote: off)]");
            }
        }
    }

    /**
     * Gets "on" or "off" string for given boolean value.
     *
     * @param b Boolean value to convert.
     * @return Result string.
     */
    private static String onOff(boolean b) {
        return b ? "on" : "off";
    }

    private static void sync(CompletableFuture<Void> future) {
        try {
            future.get();
        } catch (ExecutionException e) {
            if (hasCause(e, NodeStartException.class)) {
                sneakyThrow(e.getCause());
            }

            throw sneakyThrow(tryToCopyExceptionWithCause(e));
        } catch (InterruptedException e) {
            throw sneakyThrow(e);
        }
    }

    private static void throwIfError(Throwable exception) {
        Error error;

        try {
            error = unwrapCause(exception, Error.class);
        } catch (Throwable originalException) {
            return;
        }

        throwIfExceptionInInitializerError(error);

        throw new NodeStartException("Error occurred during node start, check .jar libraries and JVM execution arguments.", error);
    }

    private static void throwIfExceptionInInitializerError(Error error) {
        ExceptionInInitializerError initializerError;
        try {
            initializerError = unwrapCause(error, ExceptionInInitializerError.class);
        } catch (Error otherError) {
            return;
        }

        Throwable initializerErrorCause = initializerError.getCause();

        if (initializerErrorCause == null) {
            throw new NodeStartException(
                    "Error during static components initialization with unknown cause, check .jar libraries and JVM execution arguments.",
                    initializerError
            );
        }

        if (initializerErrorCause instanceof IllegalAccessException) {
            throw new NodeStartException(
                    "Error during static components initialization due to illegal code access, check --add-opens JVM execution arguments.",
                    initializerErrorCause
            );
        }

        throw new NodeStartException(
                "Error during static components initialization, check .jar libraries and JVM execution arguments.",
                initializerErrorCause
        );
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
     * Returns future that gets completed when restart is complete ({@code null} if no restart was attempted.
     */
    @TestOnly
    public @Nullable CompletableFuture<Void> restartFuture() {
        synchronized (restartOrShutdownMutex) {
            return restartFuture;
        }
    }

    @TestOnly
    public Path workDir() {
        return workDir;
    }
}
