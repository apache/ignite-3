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

package org.apache.ignite.internal.compute.executor.platform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;

public class DotNetComputeExecutor {
    static final String DOTNET_BINARY_PATH = resolveDotNetBinaryPath();

    private static final int PROCESS_START_TIMEOUT_MS = 5000;

    /** Thread-safe secure random */
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final PlatformComputeTransport transport;

    /** Single-use secret to match the connection to the process. */
    private String computeExecutorId;

    /** .NET process. Uses computeExecutorId above. */
    private Process process;

    public DotNetComputeExecutor(PlatformComputeTransport transport) {
        this.transport = transport;
    }

    public Callable<CompletableFuture<ComputeJobDataHolder>> getJobCallable(
            List<String> deploymentUnitPaths,
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        return () -> executeJobAsync(deploymentUnitPaths, jobClassName, input, context);
    }

    public synchronized void stop() {
        if (process != null) {
            process.destroy();
        }
    }

    private CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            List<String> deploymentUnitPaths,
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        if (context.isCancelled()) {
            // TODO IGNITE-25153 Platform job cancellation.
            return CompletableFuture.failedFuture(new CancellationException("Job was cancelled"));
        }

        Entry<Process, String> procEntry = ensureProcessStarted();
        Process proc = procEntry.getKey();
        String executorId = procEntry.getValue();

        // TODO: Refactor:
        // 1. Register a single-use ID once
        // 2. On timeout or process crash, try again a few times.
        return transport
                .registerComputeExecutorId(executorId)
                .orTimeout(PROCESS_START_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .exceptionally(e -> {
                    throw handleTransportError(e, proc);
                })
                .thenCompose(conn -> conn.executeJobAsync(deploymentUnitPaths, jobClassName, input));
    }

    private static RuntimeException handleTransportError(Throwable e, Process proc) {
        if (proc.isAlive()) {
            // Process is alive but did not communicate back to the server.
            proc.destroyForcibly();
            return new RuntimeException(".NET executor process failed to establish connection with the server" , e);
        } else {
            try {
                var output = new String(proc.getErrorStream().readAllBytes());

                throw new RuntimeException(".NET executor process failed to start: " + output, e);
            } catch (IOException ex) {
                RuntimeException err = new RuntimeException(
                        ".NET executor process failed to start, could not read process output: " + e.getMessage(), e);

                err.addSuppressed(ex);

                return err;
            }
        }
    }

    private synchronized Map.Entry<Process, String> ensureProcessStarted() {
        if (process == null || !process.isAlive()) {
            // Generate a new id for every new process to prevent replay attacks.
            computeExecutorId = generateSecureRandomId();
            process = startDotNetProcess(transport.serverAddress(), transport.sslEnabled(), computeExecutorId);
        }

        return Map.entry(process, computeExecutorId);
    }

    @SuppressWarnings("UseOfProcessBuilder")
    static Process startDotNetProcess(String address, boolean ssl, String executorId) {
        ProcessBuilder processBuilder = new ProcessBuilder("dotnet", DOTNET_BINARY_PATH);

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
            // Release mode - dll is next to jars.
            return basePath.getParent();
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

        return new String(Base64.getEncoder().encode(randomBytes));
    }
}
