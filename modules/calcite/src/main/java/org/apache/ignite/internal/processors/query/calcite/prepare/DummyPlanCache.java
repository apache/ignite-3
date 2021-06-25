package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.List;

public class DummyPlanCache implements QueryPlanCache {
    /** {@inheritDoc} */
    @Override public List<QueryPlan> queryPlan(PlanningContext ctx, CacheKey key, QueryPlanFactory factory) {
        return factory.create(ctx);
    }

    /** {@inheritDoc} */
    @Override public void clear() {

    }
}
