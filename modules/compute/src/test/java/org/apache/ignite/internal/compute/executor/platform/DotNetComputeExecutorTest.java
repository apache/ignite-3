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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.junit.jupiter.api.Test;

public class DotNetComputeExecutorTest {
    @Test
    public void dotNetBinaryPathExists() {
        // Check if the DotNet binary path exists.
        String dotNetBinaryPath = DotNetComputeExecutor.DOTNET_BINARY_PATH;
        assert dotNetBinaryPath != null : "DotNet binary path is null";
        assert !dotNetBinaryPath.isEmpty() : "DotNet binary path is empty";

        assertTrue(new File(dotNetBinaryPath).exists(), "File does not exist: " + dotNetBinaryPath);
    }

    public void startDotNetProcessThrowsOnWrongAddress() {
        Process proc = DotNetComputeExecutor.startDotNetProcess("foobar", false, "123");
    }
}
