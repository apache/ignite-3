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

package org.apache.ignite.internal.sql.engine.planner;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.sql.engine.externalize.RelJsonWriter.toJson;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.externalize.RelJsonReader;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.HashIndexBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.SortedIndexBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.prepare.Cloner;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlannerHelper;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.Splitter;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.StatementChecker;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * AbstractPlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class AbstractPlannerTest extends IgniteAbstractTest {
    protected static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    protected static final int DEFAULT_TBL_SIZE = 500_000;

    protected static final String DEFAULT_SCHEMA = "PUBLIC";

    protected static final int DEFAULT_ZONE_ID = 0;

    private static final AtomicInteger NEXT_TABLE_ID = new AtomicInteger(2001);

    /** Last error message. */
    String lastErrorMsg;

    interface TestVisitor {
        void visit(RelNode node, int ordinal, RelNode parent);
    }

    /**
     * TestRelVisitor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static class TestRelVisitor extends RelVisitor {
        final TestVisitor visitor;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         */
        TestRelVisitor(TestVisitor visitor) {
            this.visitor = visitor;
        }

        /** {@inheritDoc} */
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            visitor.visit(node, ordinal, parent);

            super.visit(node, ordinal, parent);
        }
    }

    protected static void relTreeVisit(RelNode n, TestVisitor v) {
        v.visit(n, -1, null);

        n.childrenAccept(new TestRelVisitor(v));
    }

    protected static IgniteDistribution someAffinity() {
        return IgniteDistributions.affinity(0, nextTableId(), DEFAULT_ZONE_ID);
    }

    protected static int nextTableId() {
        return NEXT_TABLE_ID.getAndIncrement();
    }

    /**
     * FindFirstNode.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T extends RelNode> T findFirstNode(RelNode plan, Predicate<RelNode> pred) {
        return first(findNodes(plan, pred));
    }

    /**
     * FindNodes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T extends RelNode> List<T> findNodes(RelNode plan, Predicate<RelNode> pred) {
        List<T> ret = new ArrayList<>();

        if (pred.test(plan)) {
            ret.add((T) plan);
        }

        plan.childrenAccept(
                new RelVisitor() {
                    @Override
                    public void visit(RelNode node, int ordinal, RelNode parent) {
                        if (pred.test(node)) {
                            ret.add((T) node);
                        }

                        super.visit(node, ordinal, parent);
                    }
                }
        );

        return ret;
    }

    /**
     * ByClass.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T extends RelNode> Predicate<RelNode> byClass(Class<T> cls) {
        return cls::isInstance;
    }

    /**
     * ByClass.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T extends RelNode> Predicate<RelNode> byClass(Class<T> cls, Predicate<RelNode> pred) {
        return node -> cls.isInstance(node) && pred.test(node);
    }

    /**
     * Create planner context for specified query.
     */
    protected PlanningContext plannerCtx(String sql, IgniteSchema publicSchema, String... disabledRules) {
        return plannerCtx(sql, Collections.singleton(publicSchema), null, List.of(), disabledRules);
    }

    private PlanningContext plannerCtx(
            String sql,
            Collection<IgniteSchema> schemas,
            HintStrategyTable hintStrategies,
            List<Object> params,
            String... disabledRules
    ) {
        PlanningContext ctx = PlanningContext.builder()
                .parentContext(baseQueryContext(schemas, hintStrategies, params.toArray()))
                .query(sql)
                .build();

        IgnitePlanner planner = ctx.planner();

        assertNotNull(planner);

        planner.setDisabledRules(Set.copyOf(Arrays.asList(disabledRules)));

        return ctx;
    }

    protected BaseQueryContext baseQueryContext(
            Collection<IgniteSchema> schemas,
            @Nullable HintStrategyTable hintStrategies,
            Object... params
    ) {
        SchemaPlus rootSchema = createRootSchema(false);
        SchemaPlus dfltSchema = null;

        for (IgniteSchema igniteSchema : schemas) {
            SchemaPlus schema = rootSchema.add(igniteSchema.getName(), igniteSchema);

            if (dfltSchema == null || DEFAULT_SCHEMA.equals(schema.getName())) {
                dfltSchema = schema;
            }
        }

        SqlToRelConverter.Config relConvCfg = FRAMEWORK_CONFIG.getSqlToRelConverterConfig();

        if (hintStrategies != null) {
            relConvCfg = relConvCfg.withHintStrategyTable(hintStrategies);
        }

        return BaseQueryContext.builder()
                .frameworkConfig(
                        newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(dfltSchema)
                                .sqlToRelConverterConfig(relConvCfg)
                                .build()
                )
                .logger(log)
                .parameters(params)
                .build();
    }

    /**
     * Returns the first found node of given class from expression tree.
     *
     * @param expr Expression to search in.
     * @param clz Target class of the node we are looking for.
     * @param <T> Type of the node we are looking for.
     * @return the first matching node in terms of DFS or null if there is no such node.
     */
    public static <T> T findFirst(RexNode expr, Class<T> clz) {
        if (clz.isAssignableFrom(expr.getClass())) {
            return clz.cast(expr);
        }

        if (!(expr instanceof RexCall)) {
            return null;
        }

        RexCall call = (RexCall) expr;

        for (RexNode op : call.getOperands()) {
            T res = findFirst(op, clz);

            if (res != null) {
                return res;
            }
        }

        return null;
    }

    /**
     * Optimize the specified query and build query physical plan for a test.
     */
    protected IgniteRel physicalPlan(
            String sql,
            IgniteSchema publicSchema,
            String... disabledRules) throws Exception {
        return physicalPlan(sql, publicSchema, null, disabledRules);
    }

    /**
     * Optimize the specified query and build query physical plan for a test.
     */
    protected IgniteRel physicalPlan(
            String sql,
            IgniteSchema publicSchema,
            @Nullable RelOptListener listener,
            String... disabledRules) throws Exception {
        return physicalPlan(sql, Collections.singleton(publicSchema), null, List.of(), listener, disabledRules);
    }

    protected IgniteRel physicalPlan(
            String sql,
            Collection<IgniteSchema> schemas,
            HintStrategyTable hintStrategies,
            List<Object> params,
            @Nullable RelOptListener listener,
            String... disabledRules
    ) throws Exception {
        PlanningContext planningContext = plannerCtx(sql, schemas, hintStrategies, params, disabledRules);
        return physicalPlan(planningContext, listener);
    }

    protected IgniteRel physicalPlan(PlanningContext ctx) throws Exception {
        return physicalPlan(ctx, null);
    }

    protected IgniteRel physicalPlan(PlanningContext ctx, @Nullable RelOptListener listener) throws Exception {
        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);
            assertNotNull(ctx.query());

            if (listener != null) {
                planner.addListener(listener);
            }

            return physicalPlan(planner, ctx.query());
        }
    }

    protected IgniteRel physicalPlan(IgnitePlanner planner, String qry) throws Exception {
        // Parse
        SqlNode sqlNode = planner.parse(qry);

        // Validate
        sqlNode = planner.validate(sqlNode);

        try {
            return PlannerHelper.optimize(sqlNode, planner);
        } catch (Throwable ex) {
            System.err.println(planner.dump());

            throw ex;
        }
    }

    /**
     * Select.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T> List<T> select(List<T> src, int... idxs) {
        ArrayList<T> res = new ArrayList<>(idxs.length);

        for (int idx : idxs) {
            res.add(src.get(idx));
        }

        return res;
    }

    /**
     * Predicate builder for "Operator has column names" condition.
     */
    protected <T extends RelNode> Predicate<T> hasColumns(String... cols) {
        return node -> {
            RelDataType rowType = node.getRowType();

            String err = "Unexpected columns [expected=" + Arrays.toString(cols) + ", actual=" + rowType.getFieldNames() + ']';

            if (rowType.getFieldCount() != cols.length) {
                lastErrorMsg = err;

                return false;
            }

            for (int i = 0; i < cols.length; i++) {
                if (!cols[i].equals(rowType.getFieldNames().get(i))) {
                    lastErrorMsg = err;

                    return false;
                }
            }

            return true;
        };
    }

    protected <T extends RelNode> void assertPlan(
            String sql,
            IgniteSchema schema,
            Predicate<T> predicate,
            String... disabledRules
    ) throws Exception {
        assertPlan(sql, Collections.singleton(schema), predicate, List.of(), disabledRules);
    }

    protected <T extends RelNode> void assertPlan(
            String sql,
            IgniteSchema schema,
            Predicate<T> predicate,
            List<Object> params
    ) throws Exception {
        assertPlan(sql, Collections.singleton(schema), predicate, params);
    }

    protected <T extends RelNode> void assertPlan(
            String sql,
            Collection<IgniteSchema> schemas,
            Predicate<T> predicate,
            String... disabledRules
    ) throws Exception {
        assertPlan(sql, schemas, predicate, null, List.of(), disabledRules);
    }

    protected <T extends RelNode> void assertPlan(
            String sql,
            Collection<IgniteSchema> schemas,
            Predicate<T> predicate,
            List<Object> params,
            String... disabledRules
    ) throws Exception {
        assertPlan(sql, schemas, predicate, null, params, disabledRules);
    }

    protected <T extends RelNode> void assertPlan(
            String sql,
            Collection<IgniteSchema> schemas,
            Predicate<T> predicate,
            HintStrategyTable hintStrategies,
            List<Object> params,
            String... disabledRules
    ) throws Exception {
        IgniteRel plan = physicalPlan(sql, schemas, hintStrategies, params, null, disabledRules);

        checkSplitAndSerialization(plan, schemas);

        try {
            if (predicate.test((T) plan)) {
                return;
            }
        } catch (Throwable th) {
            if (lastErrorMsg == null) {
                lastErrorMsg = th.getMessage();
            }

            String invalidPlanMsg = "Failed to validate plan (" + lastErrorMsg + "):\n"
                    + RelOptUtil.toString(plan, SqlExplainLevel.ALL_ATTRIBUTES);

            fail(invalidPlanMsg, th);
        }

        fail("Invalid plan (" + lastErrorMsg + "):\n"
                + RelOptUtil.toString(plan, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    /**
     * Predicate builder for "Instance of class" condition.
     */
    protected <T> Predicate<T> isInstanceOf(Class<T> cls) {
        return node -> {
            if (cls.isInstance(node)) {
                return true;
            }

            lastErrorMsg = "Unexpected node class [node=" + node + ", cls=" + cls.getSimpleName() + ']';

            return false;
        };
    }

    /**
     * Predicate builder for "Table scan with given name" condition.
     */
    protected <T extends RelNode> Predicate<IgniteTableScan> isTableScan(String tableName) {
        return isInstanceOf(IgniteTableScan.class).and(
                n -> {
                    String scanTableName = Util.last(n.getTable().getQualifiedName());

                    if (tableName.equalsIgnoreCase(scanTableName)) {
                        return true;
                    }

                    lastErrorMsg = "Unexpected table name [exp=" + tableName + ", act=" + scanTableName + ']';

                    return false;
                });
    }

    /**
     * Predicate builder for "Operator has distribution" condition.
     */
    protected <T extends IgniteRel> Predicate<IgniteRel> hasDistribution(IgniteDistribution distribution) {
        return node -> {
            if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                    && node.distribution().getType() == RelDistribution.Type.HASH_DISTRIBUTED
            ) {
                return distribution.satisfies(node.distribution());
            }

            return distribution.equals(node.distribution());
        };
    }

    /**
     * Predicate builder for "Any child satisfy predicate" condition.
     */
    protected <T extends RelNode> Predicate<RelNode> hasChildThat(Predicate<T> predicate) {
        return new Predicate<RelNode>() {
            public boolean checkRecursively(RelNode node) {
                if (predicate.test((T) node)) {
                    return true;
                }

                for (RelNode input : node.getInputs()) {
                    if (checkRecursively(input)) {
                        return true;
                    }
                }

                return false;
            }

            @Override
            public boolean test(RelNode node) {
                for (RelNode input : node.getInputs()) {
                    if (checkRecursively(input)) {
                        return true;
                    }
                }

                lastErrorMsg = "Not found child for defined condition [node=" + node + ']';

                return false;
            }
        };
    }

    /**
     * Predicate builder for "Current node or any child satisfy predicate" condition.
     */
    protected Predicate<RelNode> nodeOrAnyChild(Predicate<? extends RelNode> predicate) {
        return (Predicate<RelNode>) predicate.or(hasChildThat(predicate));
    }

    /**
     * Predicate builder for "Input with given index satisfy predicate" condition.
     */
    protected <T extends RelNode> Predicate<RelNode> input(Predicate<T> predicate) {
        return input(0, predicate);
    }

    /**
     * Predicate builder for "Input with given index satisfy predicate" condition.
     */
    protected <T extends RelNode> Predicate<RelNode> input(int idx, Predicate<T> predicate) {
        return node -> {
            int size = nullOrEmpty(node.getInputs()) ? 0 : node.getInputs().size();
            if (size <= idx) {
                lastErrorMsg = "No input for node [idx=" + idx + ", node=" + node + ']';

                return false;
            }

            return predicate.test((T) node.getInput(idx));
        };
    }

    /**
     * Creates public schema from provided tables.
     *
     * @param tbls Tables to create schema for.
     * @return Public schema.
     */
    protected static IgniteSchema createSchema(IgniteTable... tbls) {
        return createSchema(CatalogService.DEFAULT_SCHEMA_NAME, tbls);
    }

    /**
     * Creates schema with given name from provided tables.
     *
     * @param schemaName Schema name.
     * @param tbls Tables to create schema for.
     * @return Schema with given name.
     */
    protected static IgniteSchema createSchema(String schemaName, IgniteTable... tbls) {
        Map<String, Table> tableMap = Arrays.stream(tbls).collect(Collectors.toMap(IgniteTable::name, Function.identity()));

        return new IgniteSchema(schemaName, 0, tableMap);
    }

    protected void checkSplitAndSerialization(IgniteRel rel, IgniteSchema publicSchema) {
        checkSplitAndSerialization(rel, Collections.singleton(publicSchema));
    }

    protected void checkSplitAndSerialization(IgniteRel rel, Collection<IgniteSchema> schemas) {
        assertNotNull(rel);
        assertFalse(schemas.isEmpty());

        rel = Cloner.clone(rel);

        List<Fragment> fragments = new Splitter().go(rel);
        List<String> serialized = new ArrayList<>(fragments.size());

        for (Fragment fragment : fragments) {
            serialized.add(toJson(fragment.root()));
        }

        assertNotNull(serialized);

        List<String> nodes = new ArrayList<>(4);

        for (int i = 0; i < 4; i++) {
            nodes.add(UUID.randomUUID().toString());
        }

        List<RelNode> deserializedNodes = new ArrayList<>();

        BaseQueryContext ctx = baseQueryContext(schemas, null);

        for (String s : serialized) {
            RelJsonReader reader = new RelJsonReader(ctx.catalogReader());
            deserializedNodes.add(reader.read(s));
        }

        List<RelNode> expectedRels = fragments.stream()
                .map(Fragment::root)
                .collect(Collectors.toList());

        assertEquals(expectedRels.size(), deserializedNodes.size(), "Invalid deserialization fragments count");

        for (int i = 0; i < expectedRels.size(); ++i) {
            RelNode expected = expectedRels.get(i);
            RelNode deserialized = deserializedNodes.get(i);

            clearTraits(expected);
            clearTraits(deserialized);

            // Hints are not serializable.
            clearHints(expected);

            if (!expected.deepEquals(deserialized)) {
                assertTrue(
                        expected.deepEquals(deserialized),
                        "Invalid serialization / deserialization.\n"
                                + "Expected:\n" + RelOptUtil.toString(expected)
                                + "Deserialized:\n" + RelOptUtil.toString(deserialized)
                );
            }
        }
    }

    /**
     * Predicate builder for "Index scan with given name" condition.
     */
    protected <T extends RelNode> Predicate<IgniteIndexScan> isIndexScan(String tableName, String idxName) {
        return isInstanceOf(IgniteIndexScan.class).and(
                n -> {
                    String scanTableName = Util.last(n.getTable().getQualifiedName());

                    if (!tableName.equalsIgnoreCase(scanTableName)) {
                        lastErrorMsg = "Unexpected table name [exp=" + tableName + ", act=" + scanTableName + ']';

                        return false;
                    }

                    if (!idxName.equals(n.indexName())) {
                        lastErrorMsg = "Unexpected index name [exp=" + idxName + ", act=" + n.indexName() + ']';

                        return false;
                    }

                    return true;
                });
    }

    protected void clearTraits(RelNode rel) {
        IgniteTestUtils.setFieldValue(rel, AbstractRelNode.class, "traitSet", RelTraitSet.createEmpty());
        rel.getInputs().forEach(this::clearTraits);
    }

    protected void clearHints(RelNode rel) {
        if (rel instanceof TableScan) {
            IgniteTestUtils.setFieldValue(rel, TableScan.class, "hints", ImmutableList.of());
        }

        rel.getInputs().forEach(this::clearHints);
    }

    /**
     * TestTableDescriptor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static class TestTableDescriptor implements TableDescriptor {
        private final Supplier<IgniteDistribution> distributionSupp;

        private final RelDataType rowType;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         */
        public TestTableDescriptor(Supplier<IgniteDistribution> distribution, RelDataType rowType) {
            this.distributionSupp = distribution;
            this.rowType = rowType;
        }

        /** {@inheritDoc} */
        @Override
        public IgniteDistribution distribution() {
            return distributionSupp.get();
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
            return rowType;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
            return true;
        }

        /** {@inheritDoc} */
        @Override
        public ColumnDescriptor columnDescriptor(String fieldName) {
            RelDataTypeField field = rowType.getField(fieldName, false, false);

            NativeType nativeType = field.getType() instanceof BasicSqlType ? IgniteTypeFactory.relDataTypeToNative(field.getType()) : null;

            return new TestColumnDescriptor(field.getIndex(), fieldName, nativeType);
        }

        /** {@inheritDoc} */
        @Override
        public ColumnDescriptor columnDescriptor(int idx) {
            RelDataTypeField field = rowType.getFieldList().get(idx);

            NativeType nativeType = field.getType() instanceof BasicSqlType ? IgniteTypeFactory.relDataTypeToNative(field.getType()) : null;

            return new TestColumnDescriptor(field.getIndex(), field.getName(), nativeType);
        }

        /** {@inheritDoc} */
        @Override
        public int columnsCount() {
            return rowType.getFieldCount();
        }

        /** {@inheritDoc} */
        @Override
        public boolean isGeneratedAlways(RelOptTable table, int idxColumn) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public ColumnStrategy generationStrategy(RelOptTable table, int idxColumn) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public RexNode newColumnDefaultValue(RelOptTable table, int idxColumn, InitializerContext context) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public BiFunction<InitializerContext, RelNode, RelNode> postExpressionConversionHook() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public RexNode newAttributeInitializer(RelDataType type, SqlFunction constructor, int idxAttribute,
                List<RexNode> constructorArgs, InitializerContext context) {
            throw new AssertionError();
        }
    }

    static class TestColumnDescriptor implements ColumnDescriptor {
        private final int idx;

        private final String name;

        private final NativeType physicalType;

        TestColumnDescriptor(int idx, String name, NativeType physicalType) {
            this.idx = idx;
            this.name = name;
            this.physicalType = physicalType;
        }

        /** {@inheritDoc} */
        @Override
        public boolean nullable() {
            return true;
        }

        /** {@inheritDoc} */
        @Override
        public boolean key() {
            return false;
        }

        /** {@inheritDoc} */
        @Override
        public DefaultValueStrategy defaultStrategy() {
            return DefaultValueStrategy.DEFAULT_NULL;
        }

        /** {@inheritDoc} */
        @Override
        public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override
        public int logicalIndex() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override
        public int physicalIndex() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override
        public NativeType physicalType() {
            if (physicalType == null) {
                throw new AssertionError();
            }

            return physicalType;
        }

        /** {@inheritDoc} */
        @Override
        public Object defaultValue() {
            throw new AssertionError();
        }
    }

    static class TestSortedIndex implements SortedIndex {
        private final int id = 1;

        private final int tableId = 1;

        private final SortedIndexDescriptor descriptor;

        public static TestSortedIndex create(RelCollation collation, String name, IgniteTable table) {
            List<String> columns = new ArrayList<>();
            List<ColumnCollation> collations = new ArrayList<>();
            TableDescriptor tableDescriptor = table.descriptor();

            for (var fieldCollation : collation.getFieldCollations()) {
                columns.add(tableDescriptor.columnDescriptor(fieldCollation.getFieldIndex()).name());
                collations.add(ColumnCollation.get(
                        !fieldCollation.getDirection().isDescending(),
                        fieldCollation.nullDirection == NullDirection.FIRST
                ));
            }

            var descriptor = new SortedIndexDescriptor(name, columns, collations);

            return new TestSortedIndex(descriptor);
        }

        public TestSortedIndex(SortedIndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override
        public String name() {
            return descriptor.name();
        }

        /** {@inheritDoc} */
        @Override
        public int tableId() {
            return tableId;
        }

        /** {@inheritDoc} */
        @Override
        public SortedIndexDescriptor descriptor() {
            return descriptor;
        }

        /** {@inheritDoc} */
        @Override
        public Publisher<BinaryRow> lookup(int partId, UUID txId, PrimaryReplica recipient, BinaryTuple key,
                @Nullable BitSet columns) {
            throw new AssertionError("Should not be called");
        }

        /** {@inheritDoc} */
        @Override
        public Publisher<BinaryRow> lookup(int partId, HybridTimestamp timestamp, ClusterNode recipient, BinaryTuple key, BitSet columns) {
            throw new AssertionError("Should not be called");
        }

        /** {@inheritDoc} */
        @Override
        public Publisher<BinaryRow> scan(int partId, HybridTimestamp timestamp, ClusterNode recipient,
                @Nullable BinaryTuplePrefix leftBound, @Nullable BinaryTuplePrefix rightBound, int flags, BitSet columnsToInclude) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public Publisher<BinaryRow> scan(int partId, UUID txId, PrimaryReplica recipient, @Nullable BinaryTuplePrefix leftBound,
                @Nullable BinaryTuplePrefix rightBound, int flags, @Nullable BitSet columnsToInclude) {
            throw new AssertionError("Should not be called");
        }
    }

    /** Test Hash index implementation. */
    public static class TestHashIndex implements Index<IndexDescriptor> {
        private final int id = 1;

        private int tableId = 1;

        private final IndexDescriptor descriptor;

        /** Create index. */
        public static TestHashIndex create(List<String> indexedColumns, String name, int tableId) {
            var descriptor = new IndexDescriptor(name, indexedColumns);

            TestHashIndex idx = new TestHashIndex(descriptor);

            idx.tableId = tableId;

            return idx;
        }

        /** Create index. */
        public static TestHashIndex create(List<String> indexedColumns, String name) {
            var descriptor = new IndexDescriptor(name, indexedColumns);

            return new TestHashIndex(descriptor);
        }

        TestHashIndex(IndexDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override
        public String name() {
            return descriptor.name();
        }

        /** {@inheritDoc} */
        @Override
        public int tableId() {
            return tableId;
        }

        /** {@inheritDoc} */
        @Override
        public IndexDescriptor descriptor() {
            return descriptor;
        }

        /** {@inheritDoc} */
        @Override
        public Publisher<BinaryRow> lookup(int partId, UUID txId, PrimaryReplica recipient, BinaryTuple key,
                @Nullable BitSet columns) {
            throw new AssertionError("Should not be called");
        }

        /** {@inheritDoc} */
        @Override
        public Publisher<BinaryRow> lookup(int partId, HybridTimestamp timestamp, ClusterNode recipient, BinaryTuple key, BitSet columns) {
            throw new AssertionError("Should not be called");
        }
    }

    Predicate<SearchBounds> empty() {
        return Objects::isNull;
    }

    /**
     * Wrapper for RelOptListener with empty realization for each of their methods. Good choice in case you need implement just one or few
     * methods for listener.
     */
    static class NoopRelOptListener implements RelOptListener {

        @Override
        public void relEquivalenceFound(RelEquivalenceEvent event) {}

        @Override
        public void ruleAttempted(RuleAttemptedEvent event) {}

        @Override
        public void ruleProductionSucceeded(RuleProductionEvent event) {}

        @Override
        public void relDiscarded(RelDiscardedEvent event) {}

        @Override
        public void relChosen(RelChosenEvent event) {}
    }

    /**
     * Listener which keeps information about attempts of applying optimization rules.
     */
    public class RuleAttemptListener extends NoopRelOptListener {
        Set<RelOptRule> attemptedRules = new HashSet<>();

        @Override
        public void ruleAttempted(RuleAttemptedEvent event) {
            if (event.isBefore()) {
                attemptedRules.add(event.getRuleCall().getRule());
            }
        }

        public boolean isApplied(RelOptRule rule) {
            return attemptedRules.stream().anyMatch(r -> rule.toString().equals(r.toString()));
        }

        public void reset() {
            attemptedRules.clear();
        }
    }

    /**
     * A shorthand for {@code checkStatement().sql(statement, params)}.
     */
    public StatementChecker sql(String statement, Object... params) {
        return checkStatement().sql(statement, params);
    }

    /**
     * Creates an instance of {@link StatementChecker statement checker} to test plans.
     * <pre>
     *     checkStatement().sql("SELECT 1").ok()
     * </pre>
     */
    public StatementChecker checkStatement() {
        return new PlanChecker();
    }

    /**
     * Creates an instance of {@link PlanChecker statement checker} with the given setup.
     * A shorthand for {@code checkStatement().setup(func)}.
     */
    public StatementChecker checkStatement(Consumer<StatementChecker> setup) {
        return new PlanChecker().setup(setup);
    }

    /**
     * An implementation of {@link PlanChecker} with initialized {@link SqlPrepare} to test plans.
     */
    public class PlanChecker extends StatementChecker {

        PlanChecker() {
            super((schema, sql, params) -> {
                return physicalPlan(sql, List.of(schema), HintStrategyTable.EMPTY,
                        params, new NoopRelOptListener());
            });
        }

        /** {@inheritDoc} */
        @Override
        protected void checkRel(IgniteRel igniteRel, IgniteSchema schema) {
            checkSplitAndSerialization(igniteRel, schema);
        }
    }

    /** Creates schema using given table builders. */
    protected static IgniteSchema createSchemaFrom(Function<TableBuilder, TableBuilder>... tables) {
        return createSchema(Arrays.stream(tables).map(op -> op.apply(TestBuilders.table()).build()).toArray(IgniteTable[]::new));
    }

    /** Creates a function, which builds sorted index with given column names and with default collation. */
    protected static UnaryOperator<TableBuilder> addSortIndex(String... columns) {
        return tableBuilder -> {
            SortedIndexBuilder indexBuilder = tableBuilder.sortedIndex();
            StringBuilder nameBuilder = new StringBuilder("idx");

            for (String colName : columns) {
                indexBuilder.addColumn(colName.toUpperCase(), Collation.ASC_NULLS_LAST);
                nameBuilder.append('_').append(colName);
            }

            return indexBuilder.name(nameBuilder.toString()).end();
        };
    }

    /** Creates a function, which builds hash index with given column names. */
    protected static UnaryOperator<TableBuilder> addHashIndex(String... columns) {
        return tableBuilder -> {
            HashIndexBuilder indexBuilder = tableBuilder.hashIndex();
            StringBuilder nameBuilder = new StringBuilder("idx");

            for (String colName : columns) {
                indexBuilder.addColumn(colName.toUpperCase());
                nameBuilder.append('_').append(colName);
            }

            return indexBuilder.name(nameBuilder.toString()).end();
        };
    }

    /** Sets table size. */
    static Function<TableBuilder, TableBuilder> setSize(int size) {
        return t -> t.size(size);
    }
}
