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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;

public class SafeTimeCandidateManager {
    private final UUID uuid;
    private final HybridClock safeTimeClock;
    private final Map<Long, Map<Long, HybridTimestamp>> safeTimeCandidates = new ConcurrentHashMap<>();

    public SafeTimeCandidateManager(HybridClock safeTimeClock) {
        this(safeTimeClock, UUID.randomUUID());
    }

    public SafeTimeCandidateManager(HybridClock safeTimeClock, UUID uuid) {
        this.safeTimeClock = safeTimeClock;
        this.uuid = uuid;
        new Exception("qqq create SafeTimeCandidateManager uuid=" + uuid).printStackTrace(System.out);
    }

    public void addSafeTimeCandidate(long index, long term, HybridTimestamp safeTime) {
        System.out.println("qqq addSafeTimeCandidate uuid=" + uuid + ", index=" + index + ", term=" + term + ", safeTime=" + safeTime);
        safeTimeCandidates.compute(index, (i, candidates) -> {
            if (candidates == null) {
                candidates = new HashMap<>();
            }

            candidates.put(term, safeTime);

            return candidates;
        });
    }

    public void commitIndex(long prevIndex, long index, long term) {
        System.out.println("qqq commitIndex  uuid=" + uuid + ", prevIndex=" + prevIndex + ", index=" + index + ", term=" + term);
        long currentIndex = prevIndex + 1;

        while (currentIndex <= index) {
            Map<Long, HybridTimestamp> candidates = safeTimeCandidates.remove(currentIndex);

            if (candidates == null) {
                continue;
            }

            HybridTimestamp safeTime = null;

            for (Map.Entry<Long, HybridTimestamp> e : candidates.entrySet()) {
                if (e.getKey() == term) {
                    safeTime = e.getValue();
                }
            }

            assert safeTime != null;

            safeTimeClock.sync(safeTime);

            currentIndex++;
        }
    }
}
