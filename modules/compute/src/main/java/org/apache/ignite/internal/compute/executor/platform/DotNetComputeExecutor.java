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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;

public class DotNetComputeExecutor {
    private final PlatformComputeTransport transport;

    // TODO: Secure random.
    // TODO: Single-use secret to prevent replay attacks.
    private final String computeExecutorId = UUID.randomUUID().toString();

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

    private CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            List<String> deploymentUnitPaths,
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        if (context.isCancelled()) {
            // TODO early exit?
        }

        ensureProcessStarted();

        // TODO: Configurable timeout
        // TODO: Handle disconnects and crashed processes.
        return transport
                .getConnectionAsync(computeExecutorId)
                .orTimeout(3000, TimeUnit.MILLISECONDS)
                .thenCompose(conn -> conn.executeJobAsync(deploymentUnitPaths, jobClassName, input));
    }

    public synchronized void stop() {
        // TODO: Stop guard
        if (process != null) {
            process.destroy();
        }
    }

    private synchronized void ensureProcessStarted() {
        if (process != null && process.isAlive()) {
            return;
        }

        process = startDotNetProcess(transport.serverAddress(), transport.sslEnabled(), computeExecutorId);
    }

    @SuppressWarnings("UseOfProcessBuilder")
    private static Process startDotNetProcess(String address, boolean ssl, String executorId) {
        // TODO: Resolve relative path to the executable.
        String executorPath = "/home/pavel/w/ignite-3/modules/platforms/dotnet/"
                + "Apache.Ignite.Internal.ComputeExecutor/bin/Debug/net8.0/Apache.Ignite.Internal.ComputeExecutor.dll";

        ProcessBuilder processBuilder = new ProcessBuilder("dotnet", executorPath);

        processBuilder.environment().put("IGNITE_COMPUTE_EXECUTOR_SERVER_ADDRESS", address);
        processBuilder.environment().put("IGNITE_COMPUTE_EXECUTOR_SERVER_SSL_ENABLED", Boolean.toString(ssl));
        processBuilder.environment().put("IGNITE_COMPUTE_EXECUTOR_ID", executorId);

        try {
            return processBuilder.start();
        } catch (IOException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }
}
