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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.tracing.GridTracingManager;
import org.apache.ignite.internal.tracing.configuration.TracingConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for ExecutorService. */
@ExtendWith(ConfigurationExtension.class)
public class ExecutorTest extends IgniteAbstractTest {
    private ExecutorService executorService;

    private StripedThreadPoolExecutor stripedThreadPoolExecutor;

    @InjectConfiguration
    private TracingConfiguration tracingConfiguration;

    @BeforeEach
    void before() {
        GridTracingManager.initialize("ignite-node-0", tracingConfiguration);

        assertThat(tracingConfiguration.change(tracingChange -> {
            tracingChange.changeRatio(1.);
        }), CompletableFutureMatcher.willCompleteSuccessfully());

        executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("single-thread-pool", log));

        stripedThreadPoolExecutor = new StripedThreadPoolExecutor(
                2,
                new NamedThreadFactory("striped-thread-pool", log),
                false,
                0
        );
    }

    @AfterEach
    void after() {
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
