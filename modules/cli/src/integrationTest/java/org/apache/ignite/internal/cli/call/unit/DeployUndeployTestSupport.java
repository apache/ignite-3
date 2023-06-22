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

package org.apache.ignite.internal.cli.call.unit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;

class DeployUndeployTestSupport {
    static Path createEmptyFileIn(Path workDir) {
        try {
            return Files.createTempFile(workDir, "empty", ".txt");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static ProgressTracker tracker() {
        return new ProgressTracker() {

            @Override
            public void track(long size) {
            }

            @Override
            public void maxSize(long size) {
            }

            @Override
            public void done() {
            }

            @Override
            public void close() {
            }
        };
    }

    static <T> T get(CompletableFuture<T> future) {
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
