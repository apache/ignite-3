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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ComputeJobDataType;

public class DotNetComputeExecutor {
    private final PlatformComputeTransport transport;

    private final String connectionId = UUID.randomUUID().toString();

    private Process process;

    public DotNetComputeExecutor(PlatformComputeTransport transport) {
        this.transport = transport;
    }

    public Callable<CompletableFuture<ComputeJobDataHolder>> getJobCallable(
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        // TODO: Call into .NET process:
        // Send client port, job class name, and job data to .NET process.
        return () -> executeJobAsync(jobClassName, input, context);
    }

    private CompletableFuture<ComputeJobDataHolder> executeJobAsync(
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        ensureProcessStarted();

        // TODO: Wait for connection with some timeout.
        PlatformComputeConnection connection = transport.getConnection(connectionId);

        // TODO: Ser/de, add class name and deployment unit info.
        return connection.sendMessage(input.data())
                .thenApply(response -> new ComputeJobDataHolder(ComputeJobDataType.TUPLE, response));
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

        process = startDotNetProcess(transport.serverAddress(), connectionId);

        // TODO: Wait for the process to start and connect.
        // We need access to ClientInboundMessageHandler to do that, through some interface.
    }

    @SuppressWarnings("UseOfProcessBuilder")
    private static Process startDotNetProcess(String address, String secret) {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "dotnet",
                "Apache.Ignite.Server.Internal",
                "--",
                address,
                secret);

        try {
            return processBuilder.start();
        } catch (IOException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }
}
