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
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.network.NetworkAddress;

public class DotNetComputeExecutor {
    private final Ignite ignite;

    private final Supplier<NetworkAddress> clientAddressSupplier;

    private final String secret = UUID.randomUUID().toString();

    private Process process;

    public DotNetComputeExecutor(Ignite ignite, Supplier<NetworkAddress> clientAddressSupplier) {
        this.ignite = ignite;
        this.clientAddressSupplier = clientAddressSupplier;
    }

    public Callable<CompletableFuture<ComputeJobDataHolder>> getJobCallable(
            Ignite ignite,
            String jobClassName,
            ComputeJobDataHolder input,
            JobExecutionContext context) {
        // TODO: Call into .NET process:
        // Send client port, job class name, and job data to .NET process.
        return () -> CompletableFuture.completedFuture(null);
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

        int clientPort = clientAddressSupplier.get().port();
        process = startDotNetProcess(clientPort, secret);
    }

    @SuppressWarnings("UseOfProcessBuilder")
    private static Process startDotNetProcess(int clientPort, String secret) {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "dotnet",
                "Apache.Ignite.Server.Internal",
                "--",
                String.valueOf(clientPort),
                secret);

        try {
            return processBuilder.start();
        } catch (IOException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }
}
