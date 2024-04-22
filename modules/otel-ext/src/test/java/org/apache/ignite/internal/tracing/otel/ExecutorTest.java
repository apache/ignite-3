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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for ExecutorService. */
class ExecutorTest {
    private static final IgniteLogger LOG = Loggers.forClass(ExecutorTest.class);

    private static ExecutorService executorService;

    private static StripedThreadPoolExecutor stripedThreadPoolExecutor;

    @BeforeAll
    static void setUp() {
        executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("single-thread-pool", LOG));

        stripedThreadPoolExecutor = new StripedThreadPoolExecutor(
                2,
                new NamedThreadFactory("striped-thread-pool", LOG),
                false,
                0
        );

        // TracingManager.initialize("ignite-node-0", 1.0d);
    }

    @AfterAll
    static void tearDown() {
        executorService.shutdown();
        stripedThreadPoolExecutor.shutdown();
    }

    @Test
    public void singleThreadExecutor() {
        ExecutorService executor = taskWrapping(executorService);

        rootSpan("singleThreadExecutor", (parentSpan) -> {
            try {
                executor.submit(() -> {
                    span("runnable", (span) -> {
                        assertTrue(span.isValid());
                    });
                }).get();

                executor.submit(() -> {
                    span("callable", (span) -> {
                        assertTrue(span.isValid());
                    });

                    return 0;
                }).get();

                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void stripedThreadPoolExecutor() {
        rootSpan("stripedThreadPoolExecutor", (parentSpan) -> {
            stripedThreadPoolExecutor.submit(() -> {
                span("runnable-0", (span) -> {
                    assertTrue(span.isValid());
                });
            }, 0).join();

            return null;
        });
    }
}
