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
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeConnection;
import org.apache.ignite.internal.compute.executor.platform.PlatformComputeTransport;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DotNetComputeExecutor}.
 */
public class DotNetComputeExecutorTest {
    @Test
    public void startDotNetProcessThrowsOnBadPath() throws Exception {
        Process proc = DotNetComputeExecutor
                .startDotNetProcess("127.0.0.1:12345", false, "123", "my.dll")
                .onExit()
                .orTimeout(9, TimeUnit.SECONDS)
                .join();

        assertEquals(1, proc.exitValue());

        String result = new String(proc.getInputStream().readAllBytes(), UTF_8);
        assertThat(result, containsString("file was not found"));
    }

    @Test
    public void beginUndeployUnitDoesNotStartProcess() {
        DotNetComputeExecutor executor = new DotNetComputeExecutor(new NoOpTransport());

        executor.beginUndeployUnit(Path.of("my.dll"));

        assertNull(IgniteTestUtils.getFieldValue(executor, "process"));
    }

    private static class NoOpTransport implements PlatformComputeTransport {
        @Override
        public String serverAddress() {
            return "";
        }

        @Override
        public boolean sslEnabled() {
            return false;
        }

        @Override
        public CompletableFuture<PlatformComputeConnection> registerComputeExecutorId(String computeExecutorId) {
            return nullCompletedFuture();
        }
    }
}
