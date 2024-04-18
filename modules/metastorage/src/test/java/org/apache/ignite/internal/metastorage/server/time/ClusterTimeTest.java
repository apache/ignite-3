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

package org.apache.ignite.internal.metastorage.server.time;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl.SyncTimeAction;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link ClusterTimeImpl}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ClusterTimeTest extends BaseIgniteAbstractTest {
    private final ClusterTimeImpl clusterTime = new ClusterTimeImpl("foo", new IgniteSpinBusyLock(), new HybridClockImpl());

    @AfterEach
    void tearDown() {
        // Stop the time and verify that all internal scheduled tasks do not impede the stop process.
        assertTimeout(Duration.ofSeconds(1), clusterTime::close);
    }

    @Test
    void testWaitFor() {
        HybridTimestamp now = clusterTime.now();

        CompletableFuture<Void> future = clusterTime.waitFor(now);

        clusterTime.updateSafeTime(now);

        assertThat(future, willCompleteSuccessfully());
    }

    @Test
    void testWaitForCancellation() throws Exception {
        HybridTimestamp now = clusterTime.now();

        CompletableFuture<Void> future = clusterTime.waitFor(now);

        clusterTime.close();

        assertThat(future, willThrow(TrackerClosedException.class));
    }

    @Test
    void testIdleSafeTimeScheduler(@InjectConfiguration("mock.idleSyncTimeInterval=1") MetaStorageConfiguration config) {
        SyncTimeAction action = mock(SyncTimeAction.class);

        when(action.syncTime(any())).thenReturn(nullCompletedFuture());

        clusterTime.startSafeTimeScheduler(action, config);

        verify(action, timeout(100).atLeast(3)).syncTime(any());
    }

    @Test
    void testIdleSafeTimeSchedulerStop(@InjectConfiguration("mock.idleSyncTimeInterval=1") MetaStorageConfiguration config) {
        SyncTimeAction action = mock(SyncTimeAction.class);

        when(action.syncTime(any())).thenReturn(nullCompletedFuture());

        clusterTime.startSafeTimeScheduler(action, config);

        verify(action, timeout(100).atLeast(1)).syncTime(any());

        clusterTime.stopSafeTimeScheduler();

        clearInvocations(action);

        verify(action, after(100).never()).syncTime(any());
    }

    /**
     * Tests that {@link ClusterTimeImpl#adjust} re-schedules the idle time sync timer.
     */
    @Test
    void testSchedulerProlongation(@InjectConfiguration("mock.idleSyncTimeInterval=250") MetaStorageConfiguration config) {
        assertDoesNotThrow(() -> clusterTime.adjust(clusterTime.now()));

        SyncTimeAction action = mock(SyncTimeAction.class);

        when(action.syncTime(any())).thenReturn(nullCompletedFuture());

        clusterTime.startSafeTimeScheduler(action, config);

        verify(action, after(150).never()).syncTime(any());

        clusterTime.adjust(clusterTime.now());

        verify(action, after(150).never()).syncTime(any());

        verify(action, after(250)).syncTime(any());
    }
}
