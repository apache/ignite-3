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

package org.apache.ignite.internal.tracing.otel;

import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.apache.ignite.internal.tracing.TracingManager.taskWrapping;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.junit.jupiter.api.Test;

/** Tests for ExecutorService. */
public class ExecutorTest {
    private static final IgniteLogger LOG = Loggers.forClass(ExecutorTest.class);

    @Test
    public void stripedThreadPoolExecutor() throws ExecutionException, InterruptedException {
        var executor = taskWrapping(Executors.newSingleThreadExecutor(new NamedThreadFactory("cli-check-connection-thread", LOG)));

        rootSpan("run", (parentSpan) -> {
            return executor.submit(() -> {
                span("process", (span) -> {
                    assertTrue(span.isValid());
                });
            }, 0);
        }).get();
    }
}
