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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.prepare.PlanId;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;

/**
 * Cache for mapped fragments.
 */
public class FragmentsCache {
    private final Cache<PlanId, CacheValue> cache;

    /** Constructs cache. */
    public FragmentsCache(CacheFactory cacheFactory, int size) {
        cache = cacheFactory.create(size);
    }

    /** Computes new value if the key is absent in the cache. */
    public CacheValue computeIfAbsent(PlanId planId, Function<PlanId, CacheValue> computeFunc) {
        return cache.asMap().computeIfAbsent(planId, computeFunc);
    }

    /** Invalidates cache entry by cache value id. */
    public void invalidateById(int id) {
        List<PlanId> toInvalidate = new ArrayList<>();

        for (Map.Entry<PlanId, CacheValue> e : cache.asMap().entrySet()) {
            if (e.getValue().ids.contains(id)) {
                toInvalidate.add(e.getKey());
            }
        }

        toInvalidate.forEach(cache.asMap().keySet()::remove);
    }

    /** Invalidates cache entries. */
    public void clear() {
        cache.clear();
    }

    static class CacheValue {
        private final Set<Integer> ids;
        private final CompletableFuture<List<MappedFragment>> mapping;

        CacheValue(Set<Integer> ids, CompletableFuture<List<MappedFragment>> mapping) {
            this.ids = ids;
            this.mapping = mapping;
        }

        public CompletableFuture<List<MappedFragment>> mapping() {
            return mapping;
        }
    }
}
