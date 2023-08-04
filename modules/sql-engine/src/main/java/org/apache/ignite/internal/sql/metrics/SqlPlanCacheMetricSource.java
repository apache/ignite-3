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

package org.apache.ignite.internal.sql.metrics;

import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.MetricSetBuilder;
import org.apache.ignite.internal.sql.engine.util.cache.StatsCounter;

/**
 * Metric source, which provides SQL plan cache metrics.
 */
public class SqlPlanCacheMetricSource extends AbstractMetricSource<SqlPlanCacheMetricSource.Holder> implements StatsCounter {
    public static final String NAME = "sql.plan.cache";

    /** Constructor. */
    public SqlPlanCacheMetricSource() {
        super(NAME);
    }

    @Override
    public void recordHits(int count) {
        Holder h = holder();

        if (h != null) {
            h.cachePlanHits.add(count);
        }
    }

    @Override
    public void recordMisses(int count) {
        Holder h = holder();

        if (h != null) {
            h.cachePlanMisses.add(count);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void init(MetricSetBuilder bldr, Holder holder) {
        bldr.register(holder.cachePlanHits);
        bldr.register(holder.cachePlanMisses);
    }

    /** {@inheritDoc} */
    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Holder.
     */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final AtomicLongMetric cachePlanHits = new AtomicLongMetric("Hits", "Cache plan hits");
        private final AtomicLongMetric cachePlanMisses = new AtomicLongMetric("Misses", "Cache plan misses");
    }
}
