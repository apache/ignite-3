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

package org.apache.ignite.internal.compute.executor.platform.dotnet;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeConnection;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeTransport;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * .NET compute executor.
 */
public class DotNetComputeExecutor {
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final String DOTNET_BINARY_PATH = resolveDotNetBinaryPath();

    private static final int PROCESS_START_TIMEOUT_MS = 5000;

    private static final int PROCESS_START_MAX_ATTEMPTS = 2;

    private final PlatformComputeTransport transport;

    private DotNetExecutorProcess process;

    /**
     * Constructor.
     *
     * @param transport Compute transport.
     */
    public DotNetComputeExecutor(PlatformComputeTransport transport) {
        assert transport != null;

        this.transport = transport;
    }

    /**
     * Creates a callable for executing a job.
     *
     * @param deploymentUnitPaths Paths to deployment units.
     * @param jobClassName Name of the job class.
     * @param input Job argument.
     * @param context Job execution context.
     * @return Callable that executes the job.
     */
    public Callable<CompletableFuture<ComputeJobDataHolder>> getJobCallable(
            List<String> deploymentUnitPaths,
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        return () -> executeJobAsync(deploymentUnitPaths, jobClassName, input, context);
    }

    /**
     * Stops the executor.
     */
    public synchronized void stop() {
        if (process != null) {
            process.process().destroy();
        }
    }

    private CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            List<String> deploymentUnitPaths,
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        if (context.isCancelled()) {
            return CompletableFuture.failedFuture(new CancellationException("Job was cancelled"));
        }

        return getPlatformComputeConnectionWithRetryAsync()
                .thenCompose(conn -> conn.executeJobAsync(deploymentUnitPaths, jobClassName, input));
    }

    private CompletableFuture<PlatformComputeConnection> getPlatformComputeConnectionWithRetryAsync() {
        CompletableFuture<PlatformComputeConnection> fut = new CompletableFuture<>();

        getPlatformComputeConnectionWithRetryAsync(fut, null);

        return fut;
    }

    private void getPlatformComputeConnectionWithRetryAsync(
            CompletableFuture<PlatformComputeConnection> fut,
            @Nullable List<Throwable> errors) {
        getPlatformComputeConnection()
                .handle((res, e) -> {
                    if (e == null) {
                        fut.complete(res);
                        return null;
                    }

                    List<Throwable> errors0 = errors == null ? new ArrayList<>() : errors;
                    errors0.add(e);

                    if (errors0.size() < PROCESS_START_MAX_ATTEMPTS) {
                        getPlatformComputeConnectionWithRetryAsync(fut, errors0);
                    } else {
                        var finalErr = new IgniteException(Common.INTERNAL_ERR, "Could not start .NET executor process in "
                                + PROCESS_START_MAX_ATTEMPTS + " attempts");

                        for (Throwable t : errors0) {
                            finalErr.addSuppressed(t);
                        }

                        fut.completeExceptionally(finalErr);
                    }

                    return null;
                });
    }

    private CompletableFuture<PlatformComputeConnection> getPlatformComputeConnection() {
        CompletableFuture<PlatformComputeConnection> fut = new CompletableFuture<>();

        DotNetExecutorProcess proc = ensureProcessStarted();

        proc.process().onExit().thenRun(() -> fut.completeExceptionally(handleTransportError(proc.process(), null)));

        proc.connectionFut()
                .orTimeout(PROCESS_START_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .handle((res, e) -> {
                    if (e == null) {
                        fut.complete(res);
                    } else {
                        fut.completeExceptionally(handleTransportError(proc.process(), e));
                    }

                    return null;
                });

        return fut;
    }

    private static Throwable handleTransportError(Process proc, @Nullable Throwable cause) {
        if (proc.isAlive()) {
            // Process is alive but did not communicate back to the server.
            proc.destroyForcibly();
        }

        String output = getProcessOutputTail(proc, 500);

        return new RuntimeException(".NET executor process failed to establish connection with the server: " + output, cause);
    }

    private static String getProcessOutputTail(Process proc, int tail) {
        try {
            InputStream procInputStream = proc.getInputStream();

            while (procInputStream.available() > tail) {
                int toSkip = procInputStream.available() - tail;
                long skipped = procInputStream.skip(toSkip);

                assert skipped == toSkip : skipped + " != " + toSkip;
            }

            return new String(procInputStream.readAllBytes(), UTF_8);
        } catch (IOException e) {
            return "Failed to read process output: " + e.getMessage();
        }
    }

    private synchronized DotNetExecutorProcess ensureProcessStarted() {
        if (process == null || !process.process().isAlive()) {
            // 0. Generate a new secure id for every new process to prevent replay attacks.
            String executorId = generateSecureRandomId();

            // 1. Register the executor id with the server. Server waits for the "special client connection".
            CompletableFuture<PlatformComputeConnection> fut = transport.registerComputeExecutorId(executorId);

            // 2. Start the process. It connects to the server, passes the id, and the server knows it is the right one.
            Process proc = startDotNetProcess(transport.serverAddress(), transport.sslEnabled(), executorId, DOTNET_BINARY_PATH);

            process = new DotNetExecutorProcess(proc, fut);
        }

        return process;
    }

    @SuppressWarnings("UseOfProcessBuilder")
    static Process startDotNetProcess(String address, boolean ssl, String executorId, String binaryPath) {
        ProcessBuilder processBuilder = new ProcessBuilder("dotnet", binaryPath);

        // Merge stdout and stderr.
        processBuilder.redirectErrorStream(true);

        processBuilder.environment().put("IGNITE_COMPUTE_EXECUTOR_SERVER_ADDRESS", address);
        processBuilder.environment().put("IGNITE_COMPUTE_EXECUTOR_SERVER_SSL_ENABLED", Boolean.toString(ssl));
        processBuilder.environment().put("IGNITE_COMPUTE_EXECUTOR_ID", executorId);

        try {
            return processBuilder.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String resolveDotNetBinaryPath() {
        return resolveDotNetBinaryDir().resolve("Apache.Ignite.Internal.ComputeExecutor.dll").normalize().toString();
    }

    private static Path resolveDotNetBinaryDir() {
        Path basePath = getCurrentClassPath();

        if (basePath.endsWith(Paths.get("modules", "compute", "build", "classes", "java", "main"))) {
            // Dev mode, class file.
            return basePath.resolve(Path.of("..", "..", "..", "..", "..", "platforms", "dotnet",
                    "Apache.Ignite.Internal.ComputeExecutor", "bin", "Debug", "net8.0"));
        } else if (basePath.endsWith("SNAPSHOT.jar")) {
            // Dev mode, jar file.
            return basePath.getParent().resolve(Path.of("..", "..", "..", "..", "platforms", "dotnet",
                    "Apache.Ignite.Internal.ComputeExecutor", "bin", "Debug", "net8.0"));
        } else {
            // Release mode - dlls are in dotnet dir next to jars.
            return basePath.getParent().resolve("dotnet");
        }
    }

    private static Path getCurrentClassPath() {
        URL url = DotNetComputeExecutor.class.getProtectionDomain().getCodeSource().getLocation();
        try {
            return Paths.get(url.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static String generateSecureRandomId() {
        byte[] randomBytes = new byte[64];
        SECURE_RANDOM.nextBytes(randomBytes);

        return new String(Base64.getEncoder().encode(randomBytes), UTF_8);
    }
}
