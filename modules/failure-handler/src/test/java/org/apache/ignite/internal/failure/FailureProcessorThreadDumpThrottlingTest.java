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

package org.apache.ignite.internal.failure;

import static org.apache.ignite.internal.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_WORKER_BLOCKED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.thread.ThreadUtils.THREAD_DUMP_MSG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link FailureProcessor} thread dump throttling.
 */
@ExtendWith(ConfigurationExtension.class)
public class FailureProcessorThreadDumpThrottlingTest extends BaseIgniteAbstractTest {
    @InjectConfiguration("mock: { "
            + "oomBufferSizeBytes=0, "
            + "dumpThreadsOnFailure=true,"
            + "dumpThreadsThrottlingTimeoutMillis=0,"
            + "handler {type=noop} }")
    private static FailureProcessorConfiguration enabledThreadDumpNoThrottlingConfiguration;

    @InjectConfiguration("mock: { "
            + "oomBufferSizeBytes=0, "
            + "dumpThreadsOnFailure=true,"
            + "dumpThreadsThrottlingTimeoutMillis=3000,"
            + "handler {type=noop} }")
    private static FailureProcessorConfiguration enabledThreadDumpWithThrottlingConfiguration;

    @InjectConfiguration("mock: { "
            + "oomBufferSizeBytes=0, "
            + "dumpThreadsOnFailure=false,"
            + "dumpThreadsThrottlingTimeoutMillis=0,"
            + "handler {type=noop} }")
    private static FailureProcessorConfiguration disabledThreadDumpConfiguration;

    /**
     * Tests that thread dumps will not be logged if 'dumpThreadsOnFailure' equals to `false`.
     */
    @Test
    public void testNoThreadDumps() {
        LogInspector logInspector = new LogInspector(
                FailureManager.class.getName(),
                evt -> evt.getMessage().getFormattedMessage().startsWith(THREAD_DUMP_MSG));

        logInspector.start();
        try {
            FailureManager failureManager = new FailureManager("test-node", disabledThreadDumpConfiguration, () -> {});

            try {
                assertThat(failureManager.startAsync(new ComponentContext()), willSucceedFast());

                FailureContext failureCtx = new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

                assertThat(failureManager.process(failureCtx), is(false));
            } finally {
                assertThat(failureManager.stopAsync(new ComponentContext()), willSucceedFast());
            }
        } finally {
            logInspector.stop();
        }

        assertThat(logInspector.isMatched(), is(false));
    }

    /**
     * Tests that thread dumps will be logged for every failure when throttling is disabled.
     */
    @Test
    public void testNoThrottling() {
        LogInspector logInspector = new LogInspector(
                FailureManager.class.getName(),
                evt -> evt.getMessage().getFormattedMessage().startsWith(THREAD_DUMP_MSG)
        );

        logInspector.start();
        try {
            testFailureProcessing(enabledThreadDumpNoThrottlingConfiguration, failureProcessor -> {
                FailureContext failureCtx = new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

                // Trigger the failure processing twice.
                assertThat(failureProcessor.process(failureCtx), is(false));

                assertThat(failureProcessor.process(failureCtx), is(false));
            });
        } finally {
            logInspector.stop();
        }

        assertThat(logInspector.timesMatched().sum(), is(2));
    }

    /**
     * Tests that thread dumps will be throttled and will be generated again after timeout exceeded.
     */
    @Test
    public void testThrottling() {
        AtomicInteger threadDumpMessageCounter = new AtomicInteger();
        AtomicInteger throttlingMessageCounter = new AtomicInteger();

        LogInspector logInspector = new LogInspector(FailureManager.class.getName());

        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith(THREAD_DUMP_MSG),
                threadDumpMessageCounter::incrementAndGet);
        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith("Thread dump is hidden due to throttling settings."),
                throttlingMessageCounter::incrementAndGet);

        logInspector.start();
        try {
            testFailureProcessing(enabledThreadDumpWithThrottlingConfiguration, failureProcessor -> {
                FailureContext failureCtx = new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

                // Trigger the failure processing twice.
                for (int i = 0; i < 2; i++) {
                    assertThat(failureProcessor.process(failureCtx), is(false));
                }

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // Trigger the failure processing twice.
                for (int i = 0; i < 2; i++) {
                    assertThat(failureProcessor.process(failureCtx), is(false));
                }
            });
        } finally {
            logInspector.stop();
        }

        assertThat(threadDumpMessageCounter.get(), is(2));
        assertThat(throttlingMessageCounter.get(), is(2));
    }

    /**
     * Tests that thread dumps will be throttled per failure type and will be generated again after timeout exceeded.
     */
    @Test
    public void testThrottlingPerFailureType() {
        AtomicInteger threadDumpMessageCounter = new AtomicInteger();
        AtomicInteger throttlingMessageCounter = new AtomicInteger();

        LogInspector logInspector = new LogInspector(FailureManager.class.getName());

        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith(THREAD_DUMP_MSG),
                threadDumpMessageCounter::incrementAndGet);
        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith("Thread dump is hidden due to throttling settings."),
                throttlingMessageCounter::incrementAndGet);

        logInspector.start();
        try {
            testFailureProcessing(enabledThreadDumpWithThrottlingConfiguration, failureProcessor -> {
                FailureContext workerBlockedFailureCtx =
                        new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

                FailureContext opTimeoutFailureCtx =
                        new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable("Failure context error"));

                for (int i = 0; i < 2; i++) {
                    failureProcessor.process(workerBlockedFailureCtx);
                    failureProcessor.process(opTimeoutFailureCtx);
                }

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                for (int i = 0; i < 2; i++) {
                    failureProcessor.process(workerBlockedFailureCtx);
                    failureProcessor.process(opTimeoutFailureCtx);
                }
            });
        } finally {
            logInspector.stop();
        }

        assertThat(threadDumpMessageCounter.get(), is(4));
        assertThat(throttlingMessageCounter.get(), is(4));
    }

    /**
     * Creates a new instance of {@link FailureManager} with the given configuration and runs the test represented by {@code test} closure.
     */
    static void testFailureProcessing(FailureProcessorConfiguration configuration, Consumer<FailureProcessor> test) {
        FailureManager failureManager = new FailureManager("test-node", configuration, () -> {});

        try {
            assertThat(failureManager.startAsync(new ComponentContext()), willSucceedFast());

            test.accept(failureManager);
        } finally {
            assertThat(failureManager.stopAsync(new ComponentContext()), willSucceedFast());
        }
    }
}
