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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.externalize.RelJsonReader;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.prepare.Cloner;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.prepare.PlannerHelper;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.Splitter;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.schema.ModifyRow;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * AbstractPlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class AbstractPlannerTest extends IgniteAbstractTest {
    protected static final IgniteTypeFactory TYPE_FACTORY = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    protected static final int DEFAULT_TBL_SIZE = 500_000;

    protected static final String DEFAULT_SCHEMA = "PUBLIC";

    /** Last error message. */
    private String lastErrorMsg;

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
        return plannerCtx(sql, Collections.singleton(publicSchema), disabledRules);
    }

    protected PlanningContext plannerCtx(String sql, Collection<IgniteSchema> schemas, String... disabledRules) {
        PlanningContext ctx = PlanningContext.builder()
                .parentContext(baseQueryContext(schemas))
                .query(sql)
                .build();

        IgnitePlanner planner = ctx.planner();

        assertNotNull(planner);

        planner.setDisabledRules(Set.copyOf(Arrays.asList(disabledRules)));

        return ctx;
    }

    protected BaseQueryContext baseQueryContext(Collection<IgniteSchema> schemas) {
        SchemaPlus rootSchema = createRootSchema(false);
        SchemaPlus dfltSchema = null;

        for (IgniteSchema igniteSchema : schemas) {
            SchemaPlus schema = rootSchema.add(igniteSchema.getName(), igniteSchema);

            if (dfltSchema == null || DEFAULT_SCHEMA.equals(schema.getName())) {
                dfltSchema = schema;
            }
        }

        return BaseQueryContext.builder()
                .frameworkConfig(
                        newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(dfltSchema)
                                .build()
                )
                .logger(log)
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
    protected IgniteRel physicalPlan(String sql, IgniteSchema publicSchema, String... disabledRules) throws Exception {
        return physicalPlan(sql, plannerCtx(sql, publicSchema, disabledRules));
    }

    protected IgniteRel physicalPlan(String sql, Collection<IgniteSchema> schemas, String... disabledRules) throws Exception {
        return physicalPlan(plannerCtx(sql, schemas, disabledRules));
    }

    protected IgniteRel physicalPlan(PlanningContext ctx) throws Exception {
        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);
            assertNotNull(ctx.query());

            return physicalPlan(ctx.query(), ctx);
        }
    }

    protected IgniteRel physicalPlan(String sql, PlanningContext ctx) throws Exception {
        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);

            String qry = ctx.query();

            assertNotNull(qry);

            // Parse
            SqlNode sqlNode = planner.parse(qry);

            // Validate
            sqlNode = planner.validate(sqlNode);

            try {
                IgniteRel rel = PlannerHelper.optimize(sqlNode, planner);

                // System.out.println(RelOptUtil.toString(rel));

                return rel;
            } catch (Throwable ex) {
                System.err.println(planner.dump());

                throw ex;
            }
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

    protected static void createTable(IgniteSchema schema, String name, RelDataType type, IgniteDistribution distr) {
        TestTable table = new TestTable(type, name) {
            @Override
            public IgniteDistribution distribution() {
                return distr;
            }
        };

        schema.addTable(name, table);
    }

    /**
     * Creates test table with given params and schema.
     *
     * @param schema Table schema.
     * @param name   Name of the table.
     * @param distr  Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string representing a column name, every
     *               even item should be a class representing column's type.
     *               E.g. {@code createTable("MY_TABLE", distribution, "ID", Integer.class, "VAL", String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    protected static TestTable createTable(IgniteSchema schema, String name, IgniteDistribution distr, Object... fields) {
        TestTable tbl = createTable(name, DEFAULT_TBL_SIZE, distr, fields);

        schema.addTable(name, tbl);

        return tbl;
    }

    /**
     * Creates test table with given params.
     *
     * @param name   Name of the table.
     * @param distr  Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string representing a column name, every
     *               even item should be a class representing column's type.
     *               E.g. {@code createTable("MY_TABLE", distribution, "ID", Integer.class, "VAL", String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    protected static TestTable createTable(String name, IgniteDistribution distr, Object... fields) {
        return createTable(name, DEFAULT_TBL_SIZE, distr, fields);
    }

    /**
     * Creates test table with given params.
     *
     * @param name   Name of the table.
     * @param size   Required size of the table.
     * @param distr  Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string representing a column name, every
     *               even item should be a class representing column's type.
     *               E.g. {@code createTable("MY_TABLE", 500, distribution, "ID", Integer.class, "VAL", String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    protected static TestTable createTable(String name, int size, IgniteDistribution distr, Object... fields) {
        if (ArrayUtils.nullOrEmpty(fields) || fields.length % 2 != 0) {
            throw new IllegalArgumentException("'fields' should be non-null array with even number of elements");
        }

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(TYPE_FACTORY);

        for (int i = 0; i < fields.length; i += 2) {
            b.add((String) fields[i], TYPE_FACTORY.createJavaType((Class<?>) fields[i + 1]));
        }

        return new TestTable(name, b.build(), size) {
            @Override
            public IgniteDistribution distribution() {
                return distr;
            }
        };
    }

    protected <T extends RelNode> void assertPlan(
            String sql,
            IgniteSchema schema,
            Predicate<T> predicate,
            String... disabledRules
    ) throws Exception {
        assertPlan(sql, Collections.singleton(schema), predicate, disabledRules);
    }

    protected <T extends RelNode> void assertPlan(
            String sql,
            Collection<IgniteSchema> schemas,
            Predicate<T> predicate,
            String... disabledRules
    ) throws Exception {
        IgniteRel plan = physicalPlan(sql, schemas, disabledRules);

        checkSplitAndSerialization(plan, schemas);

        if (!predicate.test((T) plan)) {
            String invalidPlanMsg = "Invalid plan (" + lastErrorMsg + "):\n"
                    + RelOptUtil.toString(plan, SqlExplainLevel.ALL_ATTRIBUTES);

            fail(invalidPlanMsg);
        }
    }

    /**
     * Predicate builder for "Instance of class" condition.
     */
    protected <T extends RelNode> Predicate<T> isInstanceOf(Class<T> cls) {
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
    protected static IgniteSchema createSchema(TestTable... tbls) {
        IgniteSchema schema = new IgniteSchema("PUBLIC");

        for (TestTable tbl : tbls) {
            schema.addTable(tbl.name(), tbl);
        }

        return schema;
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

        Map<UUID, IgniteTable> tableMap = new HashMap<>();

        for (IgniteSchema schema : schemas) {
            tableMap.putAll(schema.getTableNames().stream()
                    .map(schema::getTable)
                    .map(IgniteTable.class::cast)
                    .collect(Collectors.toMap(IgniteTable::id, Function.identity())));
        }

        for (String s : serialized) {
            RelJsonReader reader = new RelJsonReader(new SqlSchemaManagerImpl(tableMap));
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

    /** Test table. */
    public abstract static class TestTable implements InternalIgniteTable {
        private final String name;

        private final RelProtoDataType protoType;

        private final Map<String, IgniteIndex> indexes = new HashMap<>();

        private final double rowCnt;

        private final TableDescriptor desc;

        private final UUID id = UUID.randomUUID();

        /** Constructor. */
        public TestTable(RelDataType type) {
            this(type, 100.0);
        }

        /** Constructor. */
        public TestTable(RelDataType type, String name) {
            this(name, type, 100.0);
        }

        /** Constructor. */
        public TestTable(RelDataType type, double rowCnt) {
            this(UUID.randomUUID().toString(), type, rowCnt);
        }

        /** Constructor. */
        public TestTable(String name, RelDataType type, double rowCnt) {
            protoType = RelDataTypeImpl.proto(type);
            this.rowCnt = rowCnt;
            this.name = name;

            desc = new TestTableDescriptor(this::distribution, type);
        }

        /** {@inheritDoc} */
        @Override
        public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override
        public int version() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override
        public IgniteLogicalTableScan toRel(
                RelOptCluster cluster,
                RelOptTable relOptTbl,
                @Nullable List<RexNode> proj,
                @Nullable RexNode cond,
                @Nullable ImmutableBitSet requiredColumns
        ) {
            return IgniteLogicalTableScan.create(cluster, cluster.traitSet(), relOptTbl, proj, cond, requiredColumns);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteLogicalIndexScan toRel(
                RelOptCluster cluster,
                RelOptTable relOptTbl,
                String idxName,
                @Nullable List<RexNode> proj,
                @Nullable RexNode cond,
                @Nullable ImmutableBitSet requiredColumns
        ) {
            return IgniteLogicalIndexScan.create(cluster, cluster.traitSet(), relOptTbl, idxName, proj, cond, requiredColumns);
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet bitSet) {
            RelDataType rowType = protoType.apply(typeFactory);

            if (bitSet != null) {
                RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
                for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1)) {
                    b.add(rowType.getFieldList().get(i));
                }
                rowType = b.build();
            }

            return rowType;
        }

        /** {@inheritDoc} */
        @Override
        public Statistic getStatistic() {
            return new Statistic() {
                /** {@inheritDoc} */
                @Override
                public Double getRowCount() {
                    return rowCnt;
                }

                /** {@inheritDoc} */
                @Override
                public boolean isKey(ImmutableBitSet cols) {
                    return false;
                }

                /** {@inheritDoc} */
                @Override
                public List<ImmutableBitSet> getKeys() {
                    throw new AssertionError();
                }

                /** {@inheritDoc} */
                @Override
                public List<RelReferentialConstraint> getReferentialConstraints() {
                    throw new AssertionError();
                }

                /** {@inheritDoc} */
                @Override
                public List<RelCollation> getCollations() {
                    return Collections.emptyList();
                }

                /** {@inheritDoc} */
                @Override
                public RelDistribution getDistribution() {
                    throw new AssertionError();
                }
            };
        }

        /** {@inheritDoc} */
        @Override
        public Schema.TableType getJdbcTableType() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public boolean isRolledUp(String col) {
            return false;
        }

        /** {@inheritDoc} */
        @Override
        public boolean rolledUpColumnValidInsideAgg(
                String column,
                SqlCall call,
                SqlNode parent,
                CalciteConnectionConfig config
        ) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public ColocationGroup colocationGroup(MappingQueryContext ctx) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public IgniteDistribution distribution() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public TableDescriptor descriptor() {
            return desc;
        }

        /** {@inheritDoc} */
        @Override
        public Map<String, IgniteIndex> indexes() {
            return Collections.unmodifiableMap(indexes);
        }

        /** {@inheritDoc} */
        @Override
        public void addIndex(IgniteIndex idxTbl) {
            indexes.put(idxTbl.name(), idxTbl);
        }

        /**
         * AddIndex.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         */
        public TestTable addIndex(RelCollation collation, String name) {
            indexes.put(name, new IgniteIndex(collation, name, this));

            return this;
        }

        /**
         * AddIndex.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         */
        public TestTable addIndex(String name, int... keys) {
            addIndex(TraitUtils.createCollation(Arrays.stream(keys).boxed().collect(Collectors.toList())), name);

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public IgniteIndex getIndex(String idxName) {
            return indexes.get(idxName);
        }

        /** {@inheritDoc} */
        @Override
        public void removeIndex(String idxName) {
            throw new AssertionError();
        }

        /**
         * Get name.
         */
        public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override
        public InternalTable table() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow row, RowFactory<RowT> factory,
                @Nullable ImmutableBitSet requiredColumns) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> ModifyRow toModifyRow(ExecutionContext<RowT> ectx, RowT row, Operation op, @Nullable List<String> arg) {
            throw new AssertionError();
        }
    }

    /**
     * TestTableDescriptor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    static class TestTableDescriptor implements TableDescriptor {
        private final Supplier<IgniteDistribution> distributionSupp;

        private final RelDataType rowType;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         */
        TestTableDescriptor(Supplier<IgniteDistribution> distribution, RelDataType rowType) {
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
            return new TestColumnDescriptor(field.getIndex(), fieldName);
        }

        /** {@inheritDoc} */
        @Override
        public ColumnDescriptor columnDescriptor(int idx) {
            RelDataTypeField field = rowType.getFieldList().get(idx);

            return new TestColumnDescriptor(field.getIndex(), field.getName());
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

        TestColumnDescriptor(int idx, String name) {
            this.idx = idx;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override
        public boolean key() {
            return false;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasDefaultValue() {
            return false;
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
        public RelDataType logicalType(IgniteTypeFactory f) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public NativeType physicalType() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override
        public Object defaultValue() {
            throw new AssertionError();
        }
    }

    static class SqlSchemaManagerImpl implements SqlSchemaManager {
        private final Map<UUID, IgniteTable> tablesById;

        public SqlSchemaManagerImpl(Map<UUID, IgniteTable> tablesById) {
            this.tablesById = tablesById;
        }

        @Override
        public SchemaPlus schema(@Nullable String schema) {
            throw new AssertionError();
        }

        @Override
        public IgniteTable tableById(UUID id, int ver) {
            return tablesById.get(id);
        }
    }
}
