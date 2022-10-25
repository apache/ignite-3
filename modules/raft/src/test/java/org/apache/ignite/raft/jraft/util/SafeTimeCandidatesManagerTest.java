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

package org.apache.ignite.raft.jraft.util;

import java.time.Clock;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridClockImpl;
import org.apache.ignite.hlc.HybridTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.apache.ignite.hlc.HybridClockTestUtils.mockToEpochMilli;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class for {@link SafeTimeCandidateManager}.
 */
public class SafeTimeCandidatesManagerTest {
    /**
     * Mock of a system clock.
     */
    private static MockedStatic<Clock> clockMock;

    @AfterEach
    public void afterEach() {
        if (clockMock != null && !clockMock.isClosed()) {
            clockMock.close();
        }
    }

    @Test
    public void test() {
        clockMock = mockToEpochMilli(1);

        HybridClock safeTimeClock = new HybridClockImpl();

        SafeTimeCandidateManager safeTimeCandidateManager = new SafeTimeCandidateManager(safeTimeClock);

        safeTimeCandidateManager.addSafeTimeCandidate(1, 1, new HybridTimestamp(1, 1));

        safeTimeCandidateManager.commitIndex(0, 1, 1);
        assertEquals(new HybridTimestamp(1, 2), safeTimeClock.now());

        safeTimeCandidateManager.addSafeTimeCandidate(2, 1, new HybridTimestamp(10, 1));
        safeTimeCandidateManager.addSafeTimeCandidate(2, 2, new HybridTimestamp(100, 1));
        safeTimeCandidateManager.addSafeTimeCandidate(3, 3, new HybridTimestamp(1000, 1));

        safeTimeCandidateManager.commitIndex(1, 2, 2);
        assertEquals(new HybridTimestamp(100, 2), safeTimeClock.now());

        safeTimeCandidateManager.commitIndex(2, 3, 3);
        assertEquals(new HybridTimestamp(1000, 2), safeTimeClock.now());
    }
}
