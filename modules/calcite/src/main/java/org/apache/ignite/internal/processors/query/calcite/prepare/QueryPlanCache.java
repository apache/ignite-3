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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import org.apache.ignite.internal.processors.query.calcite.exec.LifecycleAware;

/**
 * QueryPlanCache interface.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public interface QueryPlanCache extends LifecycleAware {
    /**
     * Get query plan from cache if exists,
     * otherwise prepare plan, put to cache and return the prepared plan.
     *
     * @param ctx     Context.
     * @param key     Cache key.
     * @param factory Factory method to generate a plan on cache miss.
     * @return Query plan.
     */
    QueryPlan queryPlan(PlanningContext ctx, CacheKey key, QueryPlanFactory factory);

    /**
     * Get query plan from cache if exists, otherwise returns {@code null}.
     *
     * @param key Cache key.
     * @return Query plan.
     */
    QueryPlan queryPlan(CacheKey key);

    /**
     * Clear cache.
     */
    void clear();
}
