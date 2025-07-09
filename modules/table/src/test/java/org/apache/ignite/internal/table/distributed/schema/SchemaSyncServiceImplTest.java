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

package org.apache.ignite.internal.table.distributed.schema;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSafeTimeTracker;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaSyncServiceImplTest extends BaseIgniteAbstractTest {
    private static final long DELAY_DURATION = 500;

    @Mock
    private SchemaSafeTimeTracker schemaSafeTimeTracker;

    private final LongSupplier delayDurationMs = () -> DELAY_DURATION;

    private SchemaSyncServiceImpl schemaSyncService;

    private final HybridClock clock = new HybridClockImpl();

    @BeforeEach
    void createSchemaSyncService() {
        schemaSyncService = new SchemaSyncServiceImpl(schemaSafeTimeTracker, delayDurationMs);
    }

    @Test
    void waitsOnSchemaSafeTimeTillSchemaCompletenessSubtractingDelayDuration() {
        HybridTimestamp ts = clock.now();
        var safeTimeFuture = new CompletableFuture<Void>();

        HybridTimestamp tsMinusDelayDuration = ts.subtractPhysicalTime(delayDurationMs.getAsLong());
        when(schemaSafeTimeTracker.waitFor(tsMinusDelayDuration)).thenReturn(safeTimeFuture);

        CompletableFuture<Void> waitFuture = schemaSyncService.waitForMetadataCompleteness(ts);

        assertThat(waitFuture, is(not(completedFuture())));

        safeTimeFuture.complete(null);
        assertThat(waitFuture, willCompleteSuccessfully());
    }
}
