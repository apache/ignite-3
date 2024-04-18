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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.util.CancelFlag;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;

/**
 * Planning context.
 */
public final class PlanningContext implements Context {
    private final Context parentCtx;

    private final String qry;

    /** CancelFlag is used to post and check cancellation requests. */
    private final CancelFlag cancelFlag = new CancelFlag(new AtomicBoolean());

    /** Rules which should be excluded for planning. */
    private Function<RuleSet, RuleSet> rulesFilter;

    private IgnitePlanner planner;

    /** The maximum possible planning time. If this time is exceeded, the planning will be cancelled. */
    private final long plannerTimeout;

    /** Flag indicated if planning has been canceled due to timeout. */
    private boolean timeouted = false;

    private final Int2ObjectMap<Object> parameters;

    /** Private constructor, used by a builder. */
    private PlanningContext(
            Context parentCtx,
            String qry,
            long plannerTimeout,
            Int2ObjectMap<Object> parameters
    ) {
        this.qry = qry;
        this.parentCtx = parentCtx;

        this.plannerTimeout = plannerTimeout;
        this.parameters = parameters;
    }

    /** Get framework config. */
    public FrameworkConfig config() {
        return unwrap(BaseQueryContext.class).config();
    }

    /** Get query. */
    public String query() {
        return qry;
    }

    /** Get query parameters. */
    public Int2ObjectMap<Object> parameters() {
        return parameters;
    }

    // Helper methods

    /**  Get sql operators table. */
    public SqlOperatorTable opTable() {
        return config().getOperatorTable();
    }

    /** Get sql conformance. */
    public SqlConformance conformance() {
        return config().getParserConfig().conformance();
    }

    /** Get planning timeout in millis. */
    public long plannerTimeout() {
        return plannerTimeout;
    }

    /** Get planner. */
    public IgnitePlanner planner() {
        if (planner == null) {
            planner = new IgnitePlanner(this);
        }

        return planner;
    }

    /** Get schema name. */
    public String schemaName() {
        return schema().getName();
    }

    /** Get schema. */
    public SchemaPlus schema() {
        return config().getDefaultSchema();
    }

    /** Get type factory. */
    public IgniteTypeFactory typeFactory() {
        return IgniteTypeFactory.INSTANCE;
    }

    /** Get new catalog reader. */
    public CalciteCatalogReader catalogReader() {
        return unwrap(BaseQueryContext.class).catalogReader();
    }

    /** Get cluster based on a planner and its configuration. */
    public RelOptCluster cluster() {
        return planner().cluster();
    }

    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> clazz) {
        if (clazz == getClass()) {
            return clazz.cast(this);
        }

        if (clazz == CancelFlag.class) {
            return clazz.cast(cancelFlag);
        }

        return parentCtx.unwrap(clazz);
    }

    /** Get context builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Get rules filer. */
    public RuleSet rules(RuleSet set) {
        return rulesFilter != null ? rulesFilter.apply(set) : set;
    }

    /** Set rules filter. */
    public void rulesFilter(Function<RuleSet, RuleSet> rulesFilter) {
        this.rulesFilter = rulesFilter;
    }

    /** Set a flag indicating that the planning was canceled due to a timeout. */
    public void abortByTimeout() {
        timeouted = true;
    }

    /** Returns a flag indicates if planning has been canceled due to timeout. */
    public boolean timeouted() {
        return timeouted;
    }

    /**
     * Planner context builder.
     */
    public static class Builder {

        private Context parentCtx = Contexts.empty();

        private String qry;

        private long plannerTimeout;

        private Int2ObjectMap<Object> parameters = Int2ObjectMaps.emptyMap();

        /** Parent context. */
        public Builder parentContext(Context parentCtx) {
            this.parentCtx = parentCtx;
            return this;
        }

        /** SQL statement. */
        public Builder query(String qry) {
            this.qry = qry;
            return this;
        }

        /** Planner timeout. */
        public Builder plannerTimeout(long plannerTimeout) {
            this.plannerTimeout = plannerTimeout;
            return this;
        }

        /** Values of dynamic parameters to assist with type inference. */
        public Builder parameters(Int2ObjectMap<Object> parameters) {
            this.parameters = parameters;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public PlanningContext build() {
            return new PlanningContext(parentCtx, qry, plannerTimeout, parameters);
        }
    }
}
