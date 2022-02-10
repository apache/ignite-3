/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Implementation of {@link QueryPlanCache} that simply wraps a {@link Caffeine} cache.
 */
public class QueryPlanCacheImpl implements QueryPlanCache {
    private static final long THREAD_TIMEOUT_MS = 60_000;

    private final ConcurrentMap<CacheKey, CompletableFuture<QueryPlan>> cache;

    private final ThreadPoolExecutor planningPool;

    private final BlockingQueue<Runnable> planningTasks = new LinkedBlockingQueue<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Creates a plan cache of provided size.
     *
     * @param cacheSize Desired cache size.
     */
    public QueryPlanCacheImpl(String nodeName, int cacheSize) {
        planningPool = new ThreadPoolExecutor(
                4,
                4,
                THREAD_TIMEOUT_MS,
                TimeUnit.MILLISECONDS,
                planningTasks,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(nodeName, "sqlPlan"))
        );

        planningPool.allowCoreThreadTimeOut(true);

        cache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .<CacheKey, CompletableFuture<QueryPlan>>build()
                .asMap();
    }

    /** {@inheritDoc} */
    @Override
    public QueryPlan queryPlan(CacheKey key, Supplier<QueryPlan> planSupplier) {
        lock.readLock().lock();

        CompletableFuture<QueryPlan> planFut;
        try {
            planFut = cache.computeIfAbsent(key, k -> CompletableFuture.supplyAsync(planSupplier, planningPool));
        } finally {
            lock.readLock().unlock();
        }

        return planFut.join().copy();
    }

    /** {@inheritDoc} */
    @Override
    public QueryPlan queryPlan(CacheKey key) {
        lock.readLock().lock();

        CompletableFuture<QueryPlan> planFut;
        try {
            planFut = cache.get(key);
        } finally {
            lock.readLock().unlock();
        }

        if (planFut == null) {
            return null;
        }

        return planFut.join().copy();
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        lock.writeLock().lock();

        try {
            planningTasks.clear();
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        clear();

        planningPool.shutdownNow();
    }
}
