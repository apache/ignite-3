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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * <a href="https://issues.apache.org/jira/browse/IGNITE-27787">IGNITE-27787</a>
 * Reproducer for a startup/shutdown race where leader duties can be canceled before learner update reaches
 * {@code LogicalTopologyService#validatedNodesOnLeader()}.
 */
@ExtendWith(MockitoExtension.class)
class StandaloneMetaStorageManagerStartStopRaceTest extends BaseIgniteAbstractTest {
    @RepeatedTest(300)
    void startStopRace() {
        var manager = StandaloneMetaStorageManager.create("race-node", new HybridClockImpl());
        var componentContext = new ComponentContext();

        assertThat(manager.startAsync(componentContext), willCompleteSuccessfully());

        // Keep the window tiny but non-zero to increase interleaving variety between startup and shutdown chains.
        LockSupport.parkNanos(100_000);

        assertThat(manager.stopAsync(componentContext), willCompleteSuccessfully());
    }
}
