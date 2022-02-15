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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify implementation of the {@link QueryPlanCache}.
 */
public class QueryPlanCacheSelfTest {
    /**
     * Test ensures that plan supplier (even the blocking one) doesn't blocks clearing of the cache.
     *
     * @throws Exception If something went wrong.
     */
    @Test
    public void test() throws Exception {
        final QueryPlan plan = new TestPlan();

        QueryPlanCacheImpl cache = new QueryPlanCacheImpl("TestNode", 32);
        CountDownLatch latch = new CountDownLatch(1);

        cache.queryPlan(new CacheKey(String.valueOf(0), ""), () -> plan);

        for (char c = 1; c < 64; c++) {
            char c0 = c;
            runAsync(() -> cache.queryPlan(new CacheKey(String.valueOf(c0), ""), () -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return plan;
            }));
        }

        try {
            CompletableFuture<?> clearFut = runAsync(cache::clear);

            clearFut.get(5, TimeUnit.SECONDS);
        } finally {
            latch.countDown();
        }
    }

    private static class TestPlan implements QueryPlan {
        @Override
        public Type type() {
            return Type.QUERY;
        }

        @Override
        public QueryPlan copy() {
            return this;
        }
    }
}
