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
import static org.apache.ignite.internal.thread.ThreadOperation.PROCESS_RAFT_REQ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;
import static org.apache.ignite.internal.thread.ThreadOperation.WAIT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For {@link ExecutorServiceExtension} testing. */
public class ExecutorServiceExtensionTest {
    private static final int CPUS = Runtime.getRuntime().availableProcessors();

    @ExtendWith(ExecutorServiceExtension.class)
    static class NormalFieldInjectionTest {
        private static final String DEFAULT_THREAD_PREFIX_FORMAT = "test-NormalFieldInjectionTest-%s";

        private static final String DEFAULT_THREAD_PREFIX_FOR_METHOD_FORMAT = "test-NormalFieldInjectionTest-%s-%s";

        @InjectExecutorService
        private static ExecutorService staticExecutorServiceWithDefaults;

        @InjectExecutorService(threadCount = 2, threadPrefix = "test-foo-static-executor", allowedOperations = TX_STATE_STORAGE_ACCESS)
        private static ExecutorService staticExecutorService;

        @InjectExecutorService
        private static ScheduledExecutorService staticScheduledExecutorServiceWithDefaults;

        @InjectExecutorService(
                threadCount = 3,
                threadPrefix = "test-bar-static-executor",
                allowedOperations = {STORAGE_READ, STORAGE_WRITE}
        )
        private static ScheduledExecutorService staticScheduledExecutorService;

        @InjectExecutorService
        private ExecutorService instanceExecutorServiceWithDefaults;

        @InjectExecutorService(threadCount = 4, threadPrefix = "test-foo-instance-executor", allowedOperations = STORAGE_WRITE)
        private ExecutorService instanceExecutorService;

        @InjectExecutorService
        private ScheduledExecutorService instanceScheduledExecutorServiceWithDefaults;

        @InjectExecutorService(threadCount = 5, threadPrefix = "test-bar-instance-executor", allowedOperations = PROCESS_RAFT_REQ)
        private ScheduledExecutorService instanceScheduledExecutorService;

        @BeforeAll
        static void beforeAll(
                @InjectExecutorService
                ExecutorService staticParameterExecutorServiceWithDefaults,
                @InjectExecutorService(threadCount = 6, threadPrefix = "test-foo-static-param-executor", allowedOperations = WAIT)
                ExecutorService staticParameterExecutorService,
                @InjectExecutorService
                ScheduledExecutorService staticParameterScheduledExecutorServiceWithDefaults,
                @InjectExecutorService(threadCount = 7, threadPrefix = "test-bar-static-param-executor", allowedOperations = WAIT)
                ScheduledExecutorService staticParameterScheduledExecutorService
        ) {
            checkExecutorService(
                    staticParameterExecutorServiceWithDefaults,
                    CPUS,
                    String.format(DEFAULT_THREAD_PREFIX_FOR_METHOD_FORMAT, "beforeAll", "arg0")
            );
            checkScheduledExecutorService(
                    staticParameterScheduledExecutorServiceWithDefaults,
                    1,
                    String.format(DEFAULT_THREAD_PREFIX_FOR_METHOD_FORMAT, "beforeAll", "arg2")
            );

            checkExecutorService(staticParameterExecutorService, 6, "test-foo-static-param-executor", WAIT);
            checkScheduledExecutorService(staticParameterScheduledExecutorService, 7, "test-bar-static-param-executor", WAIT);
        }

        @Test
        void test(
                @InjectExecutorService
                ExecutorService parameterExecutorServiceWithDefaults,
                @InjectExecutorService(threadCount = 8, threadPrefix = "test-foo-param-executor", allowedOperations = WAIT)
                ExecutorService parameterExecutorService,
                @InjectExecutorService
                ScheduledExecutorService parameterScheduledExecutorServiceWithDefaults,
                @InjectExecutorService(threadCount = 9, threadPrefix = "test-bar-param-executor", allowedOperations = WAIT)
                ScheduledExecutorService parameterScheduledExecutorService
        ) {
            checkExecutorService(
                    staticExecutorServiceWithDefaults,
                    CPUS,
                    String.format(DEFAULT_THREAD_PREFIX_FORMAT, "staticExecutorServiceWithDefaults")
            );
            checkScheduledExecutorService(
                    staticScheduledExecutorServiceWithDefaults,
                    1,
                    String.format(DEFAULT_THREAD_PREFIX_FORMAT, "staticScheduledExecutorServiceWithDefaults")
            );
            checkExecutorService(
                    instanceExecutorServiceWithDefaults,
                    CPUS,
                    String.format(DEFAULT_THREAD_PREFIX_FORMAT, "test-instanceExecutorServiceWithDefaults")
            );
            checkScheduledExecutorService(
                    instanceScheduledExecutorServiceWithDefaults,
                    1,
                    String.format(DEFAULT_THREAD_PREFIX_FORMAT, "test-instanceScheduledExecutorServiceWithDefaults")
            );
            checkExecutorService(
                    parameterExecutorServiceWithDefaults,
                    CPUS,
                    String.format(DEFAULT_THREAD_PREFIX_FOR_METHOD_FORMAT, "test", "arg0")
            );
            checkScheduledExecutorService(
                    parameterScheduledExecutorServiceWithDefaults,
                    1,
                    String.format(DEFAULT_THREAD_PREFIX_FOR_METHOD_FORMAT, "test", "arg2")
            );

            checkExecutorService(staticExecutorService, 2, "test-foo-static-executor", TX_STATE_STORAGE_ACCESS);
            checkScheduledExecutorService(staticScheduledExecutorService, 3, "test-bar-static-executor", STORAGE_READ, STORAGE_WRITE);
            checkExecutorService(instanceExecutorService, 4, "test-foo-instance-executor", STORAGE_WRITE);
            checkScheduledExecutorService(instanceScheduledExecutorService, 5, "test-bar-instance-executor", PROCESS_RAFT_REQ);
            checkExecutorService(parameterExecutorService, 8, "test-foo-param-executor", WAIT);
            checkScheduledExecutorService(parameterScheduledExecutorService, 9, "test-bar-param-executor", WAIT);
        }
    }

    @ExtendWith(ExecutorServiceExtension.class)
    static class ErrorFieldInjectionTest {
        @InjectExecutorService
        private static Integer staticWrongType;

        @InjectExecutorService
        private String instanceWrongType;

        @Test
        @SuppressWarnings("PMD.UnusedFormalParameter")
        public void test(
                @InjectExecutorService
                Boolean parameterWrongType
        ) {
            fail("Should not reach here");
        }
    }

    @Test
    void testFieldInjection() {
        assertExecutesSuccessfully(NormalFieldInjectionTest.class);
    }

    @Test
    void testWrongTypeInjection() {
        assertExecutesWithFailure(
                ErrorFieldInjectionTest.class,
                instanceOf(IllegalStateException.class),
                message(m -> m.contains("Unsupported field type"))
        );
    }

    private static void checkExecutorService(
            ExecutorService service,
            int expCorePoolSize,
            String expThreadPrefix,
            ThreadOperation... expThreadOperations
    ) {
        assertThat(service, instanceOf(ThreadPoolExecutor.class));

        checkThreadPoolExecutor((ThreadPoolExecutor) service, expCorePoolSize, expThreadPrefix, expThreadOperations);
    }

    private static void checkScheduledExecutorService(
            ScheduledExecutorService service,
            int expCorePoolSize,
            String expThreadPrefix,
            ThreadOperation... expThreadOperations
    ) {
        assertThat(service, instanceOf(ScheduledThreadPoolExecutor.class));

        checkThreadPoolExecutor((ScheduledThreadPoolExecutor) service, expCorePoolSize, expThreadPrefix, expThreadOperations);
    }

    private static void checkThreadPoolExecutor(
            ThreadPoolExecutor executor,
            int expCorePoolSize,
            String expThreadPrefix,
            ThreadOperation... expThreadOperations
    ) {
        assertEquals(executor.getCorePoolSize(), expCorePoolSize);

        assertThat(
                runAsync(() -> {
                    Thread thread = Thread.currentThread();

                    assertThat(thread, instanceOf(IgniteThread.class));

                    IgniteThread thread1 = (IgniteThread) thread;

                    assertThat(thread1.getName(), startsWith(expThreadPrefix));
                    assertThat(thread1.allowedOperations(), containsInAnyOrder(expThreadOperations));
                }, executor),
                willCompleteSuccessfully()
        );
    }
}
