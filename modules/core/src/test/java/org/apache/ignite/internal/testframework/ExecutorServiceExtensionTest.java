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

package org.apache.ignite.internal.testframework;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.testframework.JunitExtensionTestUtils.assertExecutesSuccessfully;
import static org.apache.ignite.internal.testframework.JunitExtensionTestUtils.assertExecutesWithFailure;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For {@link ExecutorServiceExtension} testing. */
public class ExecutorServiceExtensionTest {
    private static final int CPU = Runtime.getRuntime().availableProcessors();

    @ExtendWith(ExecutorServiceExtension.class)
    static class NormalFieldInjectionTest {
        @InjectExecutorService
        private static ExecutorService staticExecutorServiceWithDefaults;

        @InjectExecutorService(threadCount = 2, threadPrefix = "test-foo-static-executor")
        private static ExecutorService staticExecutorService;

        @InjectExecutorService
        private static ScheduledExecutorService staticScheduledExecutorServiceWithDefaults;

        @InjectExecutorService(threadCount = 3, threadPrefix = "test-bar-static-executor")
        private static ScheduledExecutorService staticScheduledExecutorService;

        @InjectExecutorService
        private ExecutorService fieldExecutorServiceWithDefaults;

        @InjectExecutorService(threadCount = 4, threadPrefix = "test-foo-field-executor")
        private ExecutorService fieldExecutorService;

        @InjectExecutorService
        private ScheduledExecutorService fieldScheduledExecutorServiceWithDefaults;

        @InjectExecutorService(threadCount = 5, threadPrefix = "test-bar-field-executor")
        private ScheduledExecutorService fieldScheduledExecutorService;

        @Test
        void test(
                @InjectExecutorService
                ExecutorService parameterExecutorServiceWithDefaults,
                @InjectExecutorService(threadCount = 6, threadPrefix = "test-foo-param-executor")
                ExecutorService parameterExecutorService,
                @InjectExecutorService
                ScheduledExecutorService parameterScheduledExecutorServiceWithDefaults,
                @InjectExecutorService(threadCount = 7, threadPrefix = "test-bar-param-executor")
                ScheduledExecutorService parameterScheduledExecutorService
        ) {
            checkExecutorService(
                    staticExecutorServiceWithDefaults,
                    CPU,
                    "test-ExecutorService-staticExecutorServiceWithDefaults"
            );
            checkScheduledExecutorService(
                    staticScheduledExecutorServiceWithDefaults,
                    1,
                    "test-ScheduledExecutorService-staticScheduledExecutorServiceWithDefaults"
            );
            checkExecutorService(
                    fieldExecutorServiceWithDefaults,
                    CPU,
                    "test-ExecutorService-fieldExecutorServiceWithDefaults"
            );
            checkScheduledExecutorService(
                    fieldScheduledExecutorServiceWithDefaults,
                    1,
                    "test-ScheduledExecutorService-fieldScheduledExecutorServiceWithDefaults"
            );
            checkExecutorService(
                    parameterExecutorServiceWithDefaults,
                    CPU,
                    "test-ExecutorService-arg"
            );
            checkScheduledExecutorService(
                    parameterScheduledExecutorServiceWithDefaults,
                    1,
                    "test-ScheduledExecutorService-arg"
            );

            checkExecutorService(staticExecutorService, 2, "test-foo-static-executor");
            checkScheduledExecutorService(staticScheduledExecutorService, 3, "test-bar-static-executor");
            checkExecutorService(fieldExecutorService, 4, "test-foo-field-executor");
            checkScheduledExecutorService(fieldScheduledExecutorService, 5, "test-bar-field-executor");
            checkExecutorService(parameterExecutorService, 6, "test-foo-param-executor");
            checkScheduledExecutorService(parameterScheduledExecutorService, 7, "test-bar-param-executor");
        }
    }

    @ExtendWith(ExecutorServiceExtension.class)
    static class ErrorFieldInjectionTest {
        @InjectExecutorService
        private static Integer staticExecutorService;

        @InjectExecutorService
        private String fieldExecutorService;

        @Test
        public void test(
                @InjectExecutorService
                Boolean parameterExecutorService
        ) {
            fail("Should not reach here");
        }
    }

    @Test
    void testFieldInjection() {
        assertExecutesSuccessfully(NormalFieldInjectionTest.class);
    }

    @Test
    void testErrorStaticFieldInjection() {
        assertExecutesWithFailure(
                ErrorFieldInjectionTest.class,
                instanceOf(AssertionError.class),
                message(m -> m.contains("Unsupported field type"))
        );
    }

    private static void checkExecutorService(ExecutorService service, int expCorePoolSize, String expThreadPrefix) {
        assertThat(service, instanceOf(ThreadPoolExecutor.class));

        checkThreadPoolExecutor((ThreadPoolExecutor) service, expCorePoolSize, expThreadPrefix);
    }

    private static void checkScheduledExecutorService(ScheduledExecutorService service, int expCorePoolSize, String expThreadPrefix) {
        assertThat(service, instanceOf(ScheduledThreadPoolExecutor.class));

        checkThreadPoolExecutor((ScheduledThreadPoolExecutor) service, expCorePoolSize, expThreadPrefix);
    }

    private static void checkThreadPoolExecutor(ThreadPoolExecutor executor, int expCorePoolSize, String expThreadPrefix) {
        assertEquals(executor.getCorePoolSize(), expCorePoolSize);

        assertThat(
                runAsync(() -> assertThat(Thread.currentThread().getName(), containsString(expThreadPrefix)), executor),
                willCompleteSuccessfully()
        );
    }
}
