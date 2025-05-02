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

import static org.apache.ignite.internal.failure.FailureProcessorThreadDumpThrottlingTest.testFailureProcessing;
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.thread.ThreadUtils.THREAD_DUMP_MSG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link FailureProcessor} logging.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(FailureManagerExtension.class)
public class FailureProcessorLoggingTest extends BaseIgniteAbstractTest {
    @InjectConfiguration("mock: { "
            + "oomBufferSizeBytes=0, "
            + "dumpThreadsOnFailure=true,"
            + "handler {type=noop} }")
    private static FailureProcessorConfiguration enabledThreadDumpConfiguration;

    /**
     * Tests log message for ignored failure types.
     */
    @Test
    public void testFailureProcessorLoggedIgnoredFailureTest() {
        AtomicInteger dumpMessageCounter = new AtomicInteger();
        AtomicInteger ignoredMessageCounter = new AtomicInteger();

        LogInspector logInspector = new LogInspector(FailureManager.class.getName());

        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith(THREAD_DUMP_MSG),
                dumpMessageCounter::incrementAndGet);
        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith("Possible failure suppressed according to a configured handler"),
                ignoredMessageCounter::incrementAndGet);

        logInspector.start();

        try {
            testFailureProcessing(enabledThreadDumpConfiguration, failureProcessor -> {
                FailureContext timeoutFailureCtx =
                        new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable("Failure context error"));

                failureProcessor.process(timeoutFailureCtx);
            });

        } finally {
            logInspector.stop();
        }

        assertThat(dumpMessageCounter.get(), is(1));
        assertThat(ignoredMessageCounter.get(), is(1));
    }

    /**
     * Tests log message for not ignored failure types.
     */
    @Test
    @MuteFailureManagerLogging // Failure is expected.
    public void testFailureProcessorLoggedFailureTest() {
        AtomicInteger dumpMessageCounter = new AtomicInteger();
        AtomicInteger errorMessageCounter = new AtomicInteger();

        LogInspector logInspector = new LogInspector(FailureManager.class.getName());

        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith(THREAD_DUMP_MSG)
                        && evt.getLevel() == Level.ERROR,
                dumpMessageCounter::incrementAndGet);
        logInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().startsWith("Critical system error detected")
                        && evt.getLevel() == Level.ERROR,
                errorMessageCounter::incrementAndGet);

        logInspector.start();

        try {
            testFailureProcessing(enabledThreadDumpConfiguration, failureProcessor -> {
                FailureContext systemWorkerFailureCtx =
                        new FailureContext(SYSTEM_WORKER_TERMINATION, new Throwable("Failure context error"));

                failureProcessor.process(systemWorkerFailureCtx);
            });

        } finally {
            logInspector.stop();
        }

        assertThat(dumpMessageCounter.get(), is(1));
        assertThat(errorMessageCounter.get(), is(1));
    }
}
