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
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.failure.handlers.FailureHandler;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.failure.handlers.StopNodeFailureHandler;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeFailureHandlerChange;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link FailureManager}.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(FailureManagerExtension.class)
class FailureProcessorTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(value = "mock: { oomBufferSizeBytes=0, handler {type=noop} }")
    protected static FailureProcessorConfiguration failureProcessorConfiguration;

    @Test
    @MuteFailureManagerLogging // Failure is expected.
    public void testFailureProcessing() {
        FailureHandler handler = mock(FailureHandler.class);

        FailureManager failureManager = new FailureManager(handler);

        try {
            assertThat(failureManager.startAsync(new ComponentContext()), willSucceedFast());

            failureManager.process(new FailureContext(FailureType.CRITICAL_ERROR, null));

            verify(handler, times(1)).onFailure(any());
        } finally {
            assertThat(failureManager.stopAsync(new ComponentContext()), willSucceedFast());
        }
    }

    @Test
    public void testIgnoredFailureTypes() {
        FailureHandler handler = new NoOpFailureHandler();

        FailureManager failureManager = new FailureManager(handler);

        try {
            assertThat(failureManager.startAsync(new ComponentContext()), willSucceedFast());

            assertThat(failureManager.process(new FailureContext(SYSTEM_WORKER_BLOCKED, null)), is(false));

            assertThat(failureManager.process(new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, null)), is(false));
        } finally {
            assertThat(failureManager.stopAsync(new ComponentContext()), willSucceedFast());
        }
    }

    @Test
    public void testDefaultFailureHandlerConfiguration() {
        FailureManager failureManager = new FailureManager(() -> {}, failureProcessorConfiguration);

        try {
            assertThat(failureManager.startAsync(new ComponentContext()), willSucceedFast());

            assertThat(failureManager.handler(), instanceOf(NoOpFailureHandler.class));

            Set<FailureType> ignoredFailureTypes = failureManager.handler().ignoredFailureTypes();

            assertTrue(ignoredFailureTypes != null && ignoredFailureTypes.size() == 2);

            assertTrue(ignoredFailureTypes.contains(SYSTEM_WORKER_BLOCKED));
            assertTrue(ignoredFailureTypes.contains(SYSTEM_CRITICAL_OPERATION_TIMEOUT));
        } finally {
            assertThat(failureManager.stopAsync(new ComponentContext()), willSucceedFast());
        }
    }

    @Test
    public void testFailureProcessorReconfiguration() {
        FailureManager failureManager = new FailureManager(() -> {}, failureProcessorConfiguration);

        try {
            assertThat(failureManager.startAsync(new ComponentContext()), willSucceedFast());

            // Reconfigure dumpThreadsOnFailure.
            assertThat(failureManager.dumpThreadsOnFailure(), is(true));

            CompletableFuture<Void> updateFuture = failureProcessorConfiguration.change(c -> {
                c.changeDumpThreadsOnFailure(false);
                c.changeDumpThreadsThrottlingTimeoutMillis(3333L);
            });
            assertThat(updateFuture, willSucceedFast());
            assertThat(failureManager.dumpThreadsOnFailure(), is(false));
            assertThat(failureManager.dumpThreadsThrottlingTimeout(), is(3333L));

            // Reconfigure handler.
            assertThat(failureManager.handler(), isA(NoOpFailureHandler.class));

            updateFuture = failureProcessorConfiguration.change(c -> {
                StopNodeFailureHandlerChange ch = c.changeHandler().convert(StopNodeFailureHandlerChange.class);

                ch.changeIgnoredFailureTypes(SYSTEM_WORKER_TERMINATION.typeName());
            });
            assertThat(updateFuture, willSucceedFast());
            assertThat(failureManager.handler(), isA(StopNodeFailureHandler.class));
            assertThat(failureManager.handler().ignoredFailureTypes(), equalTo(Set.of(SYSTEM_WORKER_TERMINATION)));
        } finally {
            assertThat(failureManager.stopAsync(new ComponentContext()), willSucceedFast());
        }
    }
}
