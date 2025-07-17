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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaSafeTimeTrackerImplTest extends BaseIgniteAbstractTest {
    @Mock
    private ClusterTime clusterTime;

    private SchemaSafeTimeTrackerImpl tracker;

    @BeforeEach
    void createTracker() {
        tracker = new SchemaSafeTimeTrackerImpl(clusterTime);
    }

    @Test
    void recoversSchemaSafeTimeOnStartFromMsSafeTime() {
        HybridTimestamp safeTimeTs = new HybridTimestamp(1, 2);
        when(clusterTime.currentSafeTime()).thenReturn(safeTimeTs);

        assertThat(tracker.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(tracker.currentSafeTime(), is(safeTimeTs));
        assertThat(tracker.waitFor(safeTimeTs), is(completedFuture()));
    }

    @Test
    void closureCompletesFuturesWithNodeStoppingException() {
        assertThat(tracker.startAsync(new ComponentContext()), willCompleteSuccessfully());

        CompletableFuture<Void> future = tracker.waitFor(HybridTimestamp.MAX_VALUE);

        assertThat(tracker.stopAsync(), willCompleteSuccessfully());

        assertThat(future, willThrow(NodeStoppingException.class));
    }
}
