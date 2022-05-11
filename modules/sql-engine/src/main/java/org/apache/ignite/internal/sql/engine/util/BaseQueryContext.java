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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;

import com.google.common.collect.Multimap;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.UnboundMetadata;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.trait.CorrelationTraitDef;
import org.apache.ignite.internal.sql.engine.trait.DistributionTraitDef;
import org.apache.ignite.internal.sql.engine.trait.RewindabilityTraitDef;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.logger.NullLogger;

/**
 * Base query context.
 */
public final class BaseQueryContext extends AbstractQueryContext {
    public static final CalciteConnectionConfig CALCITE_CONNECTION_CONFIG;

    public static final RelOptCluster CLUSTER;

    private static final IgniteTypeFactory TYPE_FACTORY;

    private static final IgniteCostFactory COST_FACTORY = new IgniteCostFactory();

    private static final BaseQueryContext EMPTY_CONTEXT;

    private static final VolcanoPlanner DUMMY_PLANNER;

    private static final RexBuilder DFLT_REX_BUILDER;

    static {
        Properties props = new Properties();

        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(FRAMEWORK_CONFIG.getParserConfig().caseSensitive()));
        props.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
                String.valueOf(FRAMEWORK_CONFIG.getParserConfig().conformance()));
        props.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                String.valueOf(true));

        CALCITE_CONNECTION_CONFIG = new CalciteConnectionConfigImpl(props);

        EMPTY_CONTEXT = builder().build();

        DUMMY_PLANNER = new VolcanoPlanner(COST_FACTORY, EMPTY_CONTEXT) {
            @Override
            public void registerSchema(RelOptSchema schema) {
                // This method in VolcanoPlanner stores schema in hash map. It can be invoked during relational
                // operators cloning, so, can be executed even with empty context. Override it for empty context to
                // prevent memory leaks.
            }
        };

        // Dummy planner must contain all trait definitions to create singleton cluster with all default traits.
        for (RelTraitDef<?> def : EMPTY_CONTEXT.config().getTraitDefs()) {
            DUMMY_PLANNER.addRelTraitDef(def);
        }

        RelDataTypeSystem typeSys = CALCITE_CONNECTION_CONFIG.typeSystem(RelDataTypeSystem.class, FRAMEWORK_CONFIG.getTypeSystem());
        TYPE_FACTORY = new IgniteTypeFactory(typeSys);

        DFLT_REX_BUILDER = new RexBuilder(TYPE_FACTORY);

        RelOptCluster cluster = RelOptCluster.create(DUMMY_PLANNER, DFLT_REX_BUILDER);

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

    private final FrameworkConfig cfg;

    private final IgniteLogger log;

    private final IgniteTypeFactory typeFactory;

    private final RexBuilder rexBuilder;

    private final QueryCancel cancel;

    private final UUID queryId;

    private final Object[] parameters;

    private CalciteCatalogReader catalogReader;

    /**
     * Private constructor, used by a builder.
     */
    private BaseQueryContext(
            UUID queryId,
            FrameworkConfig cfg,
            QueryCancel cancel,
            Object[] parameters,
            IgniteLogger log
    ) {
        super(Contexts.chain(cfg.getContext()));

        // link frameworkConfig#context() to this.
        this.cfg = Frameworks.newConfigBuilder(cfg).context(this).build();

        this.queryId = queryId;
        this.log = log;
        this.cancel = cancel;
        this.parameters = parameters;

        RelDataTypeSystem typeSys = CALCITE_CONNECTION_CONFIG.typeSystem(RelDataTypeSystem.class, cfg.getTypeSystem());

        typeFactory = new IgniteTypeFactory(typeSys);

        rexBuilder = new RexBuilder(typeFactory);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static BaseQueryContext empty() {
        return EMPTY_CONTEXT;
    }

    public UUID queryId() {
        return queryId;
    }

    public Object[] parameters() {
        return parameters;
    }

    public FrameworkConfig config() {
        return cfg;
    }

    public IgniteLogger logger() {
        return log;
    }

    public String schemaName() {
        return schema().getName();
    }

    public SchemaPlus schema() {
        return cfg.getDefaultSchema();
    }

    public IgniteTypeFactory typeFactory() {
        return typeFactory;
    }

    public RexBuilder rexBuilder() {
        return rexBuilder;
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

        return catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(dfltSchema).path(null),
                typeFactory(), CALCITE_CONNECTION_CONFIG);
    }

    public QueryCancel cancel() {
        return cancel;
    }

    /**
     * Query context builder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder {
        private static final FrameworkConfig EMPTY_CONFIG =
                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(createRootSchema(false))
                        .traitDefs(new RelTraitDef<?>[] {
                                ConventionTraitDef.INSTANCE,
                                RelCollationTraitDef.INSTANCE,
                                DistributionTraitDef.INSTANCE,
                                RewindabilityTraitDef.INSTANCE,
                                CorrelationTraitDef.INSTANCE,
                        })
                        .build();

        private FrameworkConfig frameworkCfg = EMPTY_CONFIG;

        private QueryCancel cancel = new QueryCancel();

        private IgniteLogger log = new NullLogger();

        private UUID queryId = UUID.randomUUID();

        private Object[] parameters = ArrayUtils.OBJECT_EMPTY_ARRAY;

        public Builder frameworkConfig(FrameworkConfig frameworkCfg) {
            this.frameworkCfg = Objects.requireNonNull(frameworkCfg);
            return this;
        }

        public Builder cancel(QueryCancel cancel) {
            this.cancel = Objects.requireNonNull(cancel);
            return this;
        }

        public Builder logger(IgniteLogger log) {
            this.log = Objects.requireNonNull(log);
            return this;
        }

        public Builder queryId(UUID queryId) {
            this.queryId = Objects.requireNonNull(queryId);
            return this;
        }

        public Builder parameters(Object... parameters) {
            this.parameters = Objects.requireNonNull(parameters);
            return this;
        }

        public BaseQueryContext build() {
            return new BaseQueryContext(queryId, frameworkCfg, cancel, parameters, log);
        }
    }
}
