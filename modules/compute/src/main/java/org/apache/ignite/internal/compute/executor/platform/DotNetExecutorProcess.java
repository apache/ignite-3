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

import java.util.concurrent.CompletableFuture;

/**
 * Represents a .NET executor process.
 */
class DotNetExecutorProcess {
    /** Single-use secret to match the connection to the process. */
    private final String computeExecutorId;

    /** .NET process. Uses computeExecutorId above. */
    private final Process process;

    /** .NET process connection future. Uses computeExecutorId above. */
    private final CompletableFuture<PlatformComputeConnection> connectionFut;

    DotNetExecutorProcess(String computeExecutorId, Process process, CompletableFuture<PlatformComputeConnection> connectionFut) {
        this.computeExecutorId = computeExecutorId;
        this.process = process;
        this.connectionFut = connectionFut;
    }

    String computeExecutorId() {
        return computeExecutorId;
    }

    Process process() {
        return process;
    }

    CompletableFuture<PlatformComputeConnection> connectionFut() {
        return connectionFut;
    }
}
