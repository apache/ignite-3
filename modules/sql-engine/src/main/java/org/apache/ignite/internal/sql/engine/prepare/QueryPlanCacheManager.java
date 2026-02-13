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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlanChain.VersionItem;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Manager for query plans cache. It has two levels of cache:
 * <ul>
 *     <li>The temporary cache stores plans whose table lists are not yet known. The key includes catalog version.
 *     The value in this cache is invalidated immediately after the planning process completes, and the plan itself
 *     is moved to the stable cache.</li>
 *     <li>The stable cache is used to store records representing a chain of plans for different catalog versions.
 *     The key does not include the catalog version.</li>
 * </ul>
 */
class QueryPlanCacheManager<T extends SourcesAware> {
    private final Cache<CacheKeyBase, QueryPlanChain<CompletableFuture<T>>> stableCache;

    // Used to store plans which are being built. We don't know tables they depend on until they are ready.
    private final ConcurrentMap<CacheKey, CompletableFuture<T>> tempCache = new ConcurrentHashMap<>();

    private final CatalogChangesTrackerImpl catalogChangesTracker;

    public QueryPlanCacheManager(CatalogChangesTrackerImpl catalogChangesTracker, CacheFactory cacheFactory) {
        this.catalogChangesTracker = catalogChangesTracker;
        this.stableCache = cacheFactory.create(1000);
    }

    public @Nullable CompletableFuture<T> get(CacheKey key) {
        CompletableFuture<T> fut0 = tempCache.get(key);

        if (fut0 != null) {
            return fut0;
        }

        CacheKeyBase cacheKeyBase = new CacheKeyBase(key);

        QueryPlanChain<CompletableFuture<T>> chain = stableCache.get(cacheKeyBase);

        if (chain == null) {
            return null;
        }

        VersionItem<CompletableFuture<T>> interval = chain.get(key.catalogVersion());

        return interval == null ? null : interval.payload;
    }

    public CompletableFuture<T> get(CacheKey key, Function<CacheKey, CompletableFuture<T>> mappingFunction) {
        // until plan is not ready - we don't know tables it depends on.
        CompletableFuture<T> fut0 = tempCache.get(key);

        if (fut0 != null) {
            return fut0;
        }

        CacheKeyBase cacheKeyBase = new CacheKeyBase(key);

        QueryPlanChain<CompletableFuture<T>> chain = stableCache.get(cacheKeyBase);

        if (chain == null) {
            return putToTempCache(key, mappingFunction, cacheKeyBase);
        }

        VersionItem<CompletableFuture<T>> resFut = chain.putIfAbsent(key.catalogVersion(), () -> mappingFunction.apply(key));

        // TODO table was dropped - how to understand it from here...
        // may be pass some lambda...
        if (resFut == null) {
            return putToTempCache(key, mappingFunction, cacheKeyBase);
        }

        return resFut.payload;
    }

    @TestOnly
    @Nullable public QueryPlanChain<CompletableFuture<T>> getChain(CacheKeyBase keyBase) {
        return stableCache.get(keyBase);
    }

    public void invalidate(CacheKey cacheKey) {
        tempCache.remove(cacheKey);
        stableCache.invalidate(new CacheKeyBase(cacheKey));
    }

    public void clear() {
        tempCache.clear();
        stableCache.clear();
    }

    public void removeIfValue(Predicate<CompletableFuture<T>> valueFilter) {
        tempCache.values().removeIf(value -> {
            if (valueFilter.test(value)) {
                value.cancel(true);

                return true;
            }

            return false;
        });

        stableCache.removeIfValue(value -> valueFilter.test(value.tail().payload));
    }

    private @NotNull CompletableFuture<T> putToTempCache(CacheKey key, Function<CacheKey, CompletableFuture<T>> mappingFunction,
            CacheKeyBase cacheKeyBase) {
        CompletableFuture<T> fut = tempCache.computeIfAbsent(key, k -> mappingFunction.apply(key));

        // When plan is ready - we need to put it into a stable cache.
        fut.whenComplete(
                (planInfo, error) -> {
                    if (error == null) {
                        stableCache.get(cacheKeyBase, k -> new QueryPlanChain<>(catalogChangesTracker))
                                .putIfAbsent(key.catalogVersion(), () -> fut, planInfo.sources());
                        // cleanup temp cache
                        tempCache.remove(key);
                    }
                }
        );

        return fut;
    }
}
