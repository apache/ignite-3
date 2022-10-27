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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class for {@link SafeTimeCandidateManager}.
 */
public class SafeTimeCandidatesManagerTest {
    @Test
    public void test() {
        PendingComparableValuesTracker<HybridTimestamp> safeTimeClock = new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));

        SafeTimeCandidateManager safeTimeCandidateManager = new SafeTimeCandidateManager(safeTimeClock);

        safeTimeCandidateManager.addSafeTimeCandidate(1, 1, new HybridTimestamp(1, 1));

        safeTimeCandidateManager.commitIndex(0, 1, 1);
        assertEquals(new HybridTimestamp(1, 1), safeTimeClock.current());

        safeTimeCandidateManager.addSafeTimeCandidate(2, 1, new HybridTimestamp(10, 1));
        safeTimeCandidateManager.addSafeTimeCandidate(2, 2, new HybridTimestamp(100, 1));
        safeTimeCandidateManager.addSafeTimeCandidate(3, 3, new HybridTimestamp(1000, 1));

        safeTimeCandidateManager.commitIndex(1, 2, 2);
        assertEquals(new HybridTimestamp(100, 1), safeTimeClock.current());

        safeTimeCandidateManager.commitIndex(2, 3, 3);
        assertEquals(new HybridTimestamp(1000, 1), safeTimeClock.current());
    }
}
