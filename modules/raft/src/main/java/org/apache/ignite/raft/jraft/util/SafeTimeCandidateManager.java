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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * This manager stores safe time candidates coming with appendEntries requests, and applies them after committing corresponding
 * indexes. Only candidates with correct term can be applied, other ones are discarded.
 */
public class SafeTimeCandidateManager {
    /** Safe time clock. */
    private final PendingComparableValuesTracker<HybridTimestamp> safeTimeTracker;

    /** Candidates map. */
    private final Map<Long, Map<Long, HybridTimestamp>> safeTimeCandidates = new ConcurrentHashMap<>();

    public SafeTimeCandidateManager(PendingComparableValuesTracker<HybridTimestamp> safeTimeTracker) {
        this.safeTimeTracker = safeTimeTracker;
    }

    /**
     * Add safe time candidate.
     *
     * @param index Corresponding log index.
     * @param term Corresponding term.
     * @param safeTime Safe time candidate.
     */
    public void addSafeTimeCandidate(long index, long term, HybridTimestamp safeTime) {
        safeTimeCandidates.compute(index, (i, candidates) -> {
            if (candidates == null) {
                candidates = new HashMap<>();
            }

            candidates.put(term, safeTime);

            return candidates;
        });
    }

    /**
     * Called on index commit, applies safe time for corresponding index.
     *
     * @param prevIndex Previous applied index.
     * @param index Index.
     * @param term Term.
     */
    public void commitIndex(long prevIndex, long index, long term) {
        long currentIndex = prevIndex + 1;

        while (currentIndex <= index) {
            Map<Long, HybridTimestamp> candidates = safeTimeCandidates.remove(currentIndex);

            if (candidates != null) {
                HybridTimestamp safeTime = null;

                for (Map.Entry<Long, HybridTimestamp> e : candidates.entrySet()) {
                    if (e.getKey() == term) {
                        safeTime = e.getValue();
                    }
                }

                if (safeTime != null) {
                    safeTimeTracker.update(safeTime);
                }
            }

            currentIndex++;
        }
    }
}
