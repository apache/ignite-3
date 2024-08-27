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

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.ignite.internal.sql.engine.util.Commons.DISTRIBUTED_TRAITS_SET;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;

import com.google.common.collect.Multimap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.UnboundMetadata;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.util.CancelFlag;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.rex.IgniteRexBuilder;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Planning context.
 */
public final class PlanningContext implements Context {
    private static final CalciteConnectionConfig CALCITE_CONNECTION_CONFIG;

    public static final RelOptCluster CLUSTER;

    private static final IgniteCostFactory COST_FACTORY = new IgniteCostFactory();

    static {
        Properties props = new Properties();

        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(FRAMEWORK_CONFIG.getParserConfig().caseSensitive()));
        props.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
                String.valueOf(FRAMEWORK_CONFIG.getParserConfig().conformance()));
        props.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                String.valueOf(true));

        CALCITE_CONNECTION_CONFIG = new CalciteConnectionConfigImpl(props);

        RexBuilder defaultRexBuilder = IgniteRexBuilder.INSTANCE;

        PlanningContext emptyContext = builder().build();
        VolcanoPlanner planner = new VolcanoPlanner(COST_FACTORY, emptyContext) {
            @Override
            public void registerSchema(RelOptSchema schema) {
                // This method in VolcanoPlanner stores schema in hash map. It can be invoked during relational
                // operators cloning, so, can be executed even with empty context. Override it for empty context to
                // prevent memory leaks.
            }
        };

        // Dummy planner must contain all trait definitions to create singleton cluster with all default traits.
        for (RelTraitDef<?> def : DISTRIBUTED_TRAITS_SET) {
            planner.addRelTraitDef(def);
        }

        RelOptCluster cluster = RelOptCluster.create(planner, defaultRexBuilder);

        // Forbid using the empty cluster in any planning or mapping procedures to prevent memory leaks.
        String cantBeUsedMsg = "Empty cluster can't be used for planning or mapping";

        cluster.setMetadataProvider(
                new RelMetadataProvider() {
                    @Override
                    public <M extends Metadata> UnboundMetadata<M> apply(
                            Class<? extends RelNode> relCls,
                            Class<? extends M> metadataCls
                    ) {
                        throw new AssertionError(cantBeUsedMsg);
                    }

                    @Override
                    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(MetadataDef<M> def) {
                        throw new AssertionError(cantBeUsedMsg);
                    }

                    @Override
                    public List<MetadataHandler<?>> handlers(Class<? extends MetadataHandler<?>> hndCls) {
                        throw new AssertionError(cantBeUsedMsg);
                    }
                }
        );

        cluster.setMetadataQuerySupplier(() -> {
            throw new AssertionError(cantBeUsedMsg);
        });

        CLUSTER = cluster;
    }

    private final Context parentCtx;

    private final FrameworkConfig cfg;

    private final String qry;

    /** CancelFlag is used to post and check cancellation requests. */
    private final CancelFlag cancelFlag = new CancelFlag(new AtomicBoolean());

    /** Rules which should be excluded for planning. */
    private Function<RuleSet, RuleSet> rulesFilter;

    private IgnitePlanner planner;

    /** The maximum possible planning time. If this time is exceeded, the planning will be cancelled. */
    private final long plannerTimeout;

    /** Flag indicated if planning has been canceled due to timeout. */
    private volatile boolean timeouted;

    private final Int2ObjectMap<Object> parameters;

    private @Nullable CalciteCatalogReader catalogReader;

    private final boolean explicitTx;

    /** Private constructor, used by a builder. */
    private PlanningContext(
            FrameworkConfig config,
            String qry,
            long plannerTimeout,
            Int2ObjectMap<Object> parameters,
            boolean explicitTx
    ) {
        this.parentCtx = config.getContext();

        // link frameworkConfig#context() to this.
        this.cfg = Frameworks.newConfigBuilder(config).context(this).build();

        this.qry = qry;

        this.plannerTimeout = plannerTimeout;
        this.parameters = parameters;
        this.explicitTx = explicitTx;
    }

    /** Get framework config. */
    public FrameworkConfig config() {
        return cfg;
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
        return Objects.requireNonNull(config().getDefaultSchema());
    }

    public int catalogVersion() {
        return Objects.requireNonNull(schema().unwrap(IgniteSchema.class)).catalogVersion();
    }

    /** Get type factory. */
    public IgniteTypeFactory typeFactory() {
        return IgniteTypeFactory.INSTANCE;
    }

    /**
     * Returns calcite catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        if (catalogReader != null) {
            return catalogReader;
        }

        SchemaPlus dfltSchema = schema();
        SchemaPlus rootSchema = dfltSchema;

        while (rootSchema.getParentSchema() != null) {
            rootSchema = rootSchema.getParentSchema();
        }

        //noinspection NestedAssignment
        return catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(dfltSchema).path(null),
                IgniteTypeFactory.INSTANCE, CALCITE_CONNECTION_CONFIG);
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

    /** Returns {@code true} if planning is taking place within an explicit transaction. */
    public boolean explicitTx() {
        return explicitTx;
    }

    /**
     * Planner context builder.
     */
    public static class Builder {
        private static final FrameworkConfig EMPTY_CONFIG =
                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(createRootSchema(false))
                        .traitDefs(DISTRIBUTED_TRAITS_SET)
                        .build();

        private FrameworkConfig frameworkConfig = EMPTY_CONFIG;

        private String qry;

        private long plannerTimeout;

        private Int2ObjectMap<Object> parameters = Int2ObjectMaps.emptyMap();

        private boolean explicitTx;

        public Builder frameworkConfig(FrameworkConfig frameworkCfg) {
            this.frameworkConfig = Objects.requireNonNull(frameworkCfg);
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

        /** Sets whether explicit transaction is present. */
        public Builder explicitTx(boolean explicitTx) {
            this.explicitTx = explicitTx;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public PlanningContext build() {
            return new PlanningContext(frameworkConfig, qry, plannerTimeout, parameters, explicitTx);
        }
    }
}
