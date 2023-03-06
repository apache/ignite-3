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

import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.util.ArrayUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Base class for further planner test implementations.
 */
public abstract class AbstractAggregatePlannerTest extends AbstractPlannerTest {

    private static final Predicate<AggregateCall> NON_NULL_PREDICATE = Objects::nonNull;


    enum TestCase {
        /**
         * Query: SELECT AVG(val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_1("SELECT AVG(val0) FROM test", schema(single())),
        /**
         * Query: SELECT AVG(val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_1A("SELECT AVG(val0) FROM test", schema(hash())),
        /**
         * Query: SELECT AVG(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_2_1("SELECT AVG(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT AVG(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_2_1A("SELECT AVG(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_2_2("SELECT COUNT(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_2_2A("SELECT COUNT(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_2_3("SELECT SUM(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_2_3A("SELECT SUM(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_3_1("SELECT MIN(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_3_1A("SELECT MIN(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_3_2("SELECT MAX(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_3_2A("SELECT MAX(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_5("SELECT AVG(val0) FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash
         */
        CASE_5A("SELECT AVG(val0) FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution single
         */
        CASE_6("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(single())),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_6A("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(hash())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_7_1("SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_7_1A("SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_7_2("SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_7_2A("SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT AVG(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_7_3("SELECT AVG(DISTINCT val0) FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT AVG(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_7_3A("SELECT AVG(DISTINCT val0) FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_7_4("SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_7_4A("SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution single
         */
        CASE_8_1("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema(single())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_8_1A("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema(hash())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution single
         */
        CASE_8_2("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema(single())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_8_2A("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema(hash())),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp0.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution single
         */
        CASE_9("SELECT AVG(val0) FROM test GROUP BY grp0", schema(single(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp0.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution hash(0)
         */
        CASE_9A("SELECT AVG(val0) FROM test GROUP BY grp0", schema(hash(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp0, grp1.
         *
         * <p>Distribution single
         */
        CASE_10("SELECT AVG(val0) FROM test GROUP BY grp0, grp1", schema(single(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp0, grp1.
         *
         * <p>Distribution hash(0)
         */
        CASE_10A("SELECT AVG(val0) FROM test GROUP BY grp0, grp1", schema(hash(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution single
         */
        CASE_11("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(single(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT AVG(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_11A("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(hash(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT DISTINCT val0, val1 FROM test.
         *
         * <p>Index on val0
         *
         * <p>Distribution single
         */
        CASE_12("SELECT DISTINCT val0, val1 FROM test", schema(single(), index("val0", 1))),
        /**
         * Query: SELECT DISTINCT val0, val1 FROM test.
         *
         * <p>Index on val0
         *
         * <p>Distribution hash(0)
         */
        CASE_12A("SELECT DISTINCT val0, val1 FROM test", schema(hash(), index("val0", 1))),
        /**
         * Query: SELECT DISTINCT grp0, grp1 FROM test.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution single
         */
        CASE_13("SELECT DISTINCT grp0, grp1 FROM test", schema(single(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT DISTINCT grp0, grp1 FROM test.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution hash(0)
         */
        CASE_13A("SELECT DISTINCT grp0, grp1 FROM test", schema(hash(), index("grp0_grp1", 3, 4))),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = (SELECT AVG(val1) FROM test).
         *
         * <p>Distribution single
         */
        CASE_14("SELECT val0 FROM test WHERE VAL1 = (SELECT AVG(val1) FROM test)", schema(single())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = (SELECT AVG(val1) FROM test).
         *
         * <p>Distribution hash(0)
         */
        CASE_14A("SELECT val0 FROM test WHERE VAL1 = (SELECT AVG(val1) FROM test)", schema(hash())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test).
         *
         * <p>Distribution single
         */
        CASE_15("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", schema(single())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test).
         *
         * <p>Distribution hash(0)
         */
        CASE_15A("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", schema(hash())),
        /**
         * Query: SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test).
         *
         * <p>Index on val0 DESC
         *
         * <p>Distribution single
         */
        CASE_16("SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)", schema(single(), indexByVal0Desc())),
        /**
         * Query: SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test).
         *
         * <p>Index on val0 DESC
         *
         * <p>Distribution hash(0)
         */
        CASE_16A("SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)", schema(hash(), indexByVal0Desc())),
        /**
         * Query: SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 1) FROM test.
         *
         * <p>Distribution single
         */
        CASE_17("SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 1) FROM test", schema(single())),
        /**
         * Query: SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 1) FROM test.
         *
         * <p>Distribution hash(0)
         */
        CASE_17A("SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 1) FROM test", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1.
         *
         * <p>Distribution single
         */
        CASE_18_1("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_18_1A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0.
         *
         * <p>Distribution single
         */
        CASE_18_2("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0.
         *
         * <p>Distribution hash(0)
         */
        CASE_18_2A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1.
         *
         * <p>Distribution single
         */
        CASE_18_3("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_18_3A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1", schema(hash())),

        /**
         * SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0.
         *
         * <p>Distribution single
         */
        CASE_19_1("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0", schema(single())),
        /**
         * SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0.
         *
         * <p>Distribution hash(0)
         */
        CASE_19_1A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1.
         *
         * <p>Distribution single
         */
        CASE_19_2("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_19_2A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt.
         *
         * <p>Distribution single
         */
        CASE_20("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt.
         *
         * <p>Distribution hash(0)
         */
        CASE_20A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt", schema(hash())),
        /**
         * Query: SELECT /*+ EXPAND_DISTINCT_AGG *&#47; SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0.
         *
         * <p>Index on val0
         *
         * <p>Distribution single
         */
        CASE_21("SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0",
                schema(single(), index("idx_val0", 3, 1), index("idx_val1", 3, 2))),
        /**
         * Query: SELECT /*+ EXPAND_DISTINCT_AGG *&#47; SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0.
         *
         * <p>Index on val0
         *
         * <p>Distribution hash(0)
         */
        CASE_21A("SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0",
                schema(hash(), index("idx_val0", 3, 1), index("idx_val1", 3, 2))),
        /**
         * Query: SELECT SUM(VAL_TINYINT), SUM(VAL_SMALLINT), SUM(VAL_INT), SUM(VAL_BIGINT), SUM(VAL_DECIMAL), SUM(VAL_FLOAT),
         * SUM(VAL_DOUBLE) FROM test GROUP BY grp.
         *
         * <p>Distribution single
         */
        CASE_22("SELECT SUM(VAL_TINYINT), SUM(VAL_SMALLINT), SUM(VAL_INT), SUM(VAL_BIGINT), SUM(VAL_DECIMAL), SUM(VAL_FLOAT), "
                + "SUM(VAL_DOUBLE) FROM test GROUP BY grp", schemaWithAllNumerics(single())),
        /**
         * Query: SELECT SUM(VAL_TINYINT), SUM(VAL_SMALLINT), SUM(VAL_INT), SUM(VAL_BIGINT), SUM(VAL_DECIMAL), SUM(VAL_FLOAT),
         * SUM(VAL_DOUBLE) FROM test GROUP BY grp.
         *
         * <p>Distribution hash(0)
         */
        CASE_22A("SELECT SUM(VAL_TINYINT), SUM(VAL_SMALLINT), SUM(VAL_INT), SUM(VAL_BIGINT), SUM(VAL_DECIMAL), SUM(VAL_FLOAT), "
                + "SUM(VAL_DOUBLE) FROM test GROUP BY grp", schemaWithAllNumerics(hash())),

        ;

        final String query;
        final IgniteSchema schema;

        TestCase(String query, IgniteSchema schema) {
            this.query = query;
            this.schema = schema;
        }

        @Override
        public String toString() {
            return this.name() + ": query=" + query;
        }
    }

    static EnumSet<TestCase> missedCases;

    @BeforeAll
    static void initMissedCases() {
        missedCases = EnumSet.allOf(TestCase.class);
    }

    @AfterAll
    static void ensureAllCasesAreCovered() {
        assertThat("Some cases were not covered by test", missedCases, Matchers.empty());
    }

    /**
     * Validates SUM aggregate has a correct return type for any numeric column type.
     */
    @Test
    public void sumAggregateTypes() throws Exception {
        List<RelDataType> types0 = List.of(
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL,
                        TYPE_FACTORY.getTypeSystem().getMaxNumericPrecision(), 0), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL,
                        TYPE_FACTORY.getTypeSystem().getMaxNumericPrecision(), DecimalNativeType.DEFAULT_SCALE), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), true));

        Predicate<RelNode> pred = nodeOrAnyChild(isInstanceOf(SingleRel.class))
                .and(n -> {
                    List<RelDataType> types = n.getRowType().getFieldList().stream()
                            .map(RelDataTypeField::getType)
                            .collect(Collectors.toList());

                    return types.equals(types0);
                });

        assertPlan(TestCase.CASE_22, pred);
        assertPlan(TestCase.CASE_22A, pred);
    }

    /**
     * Rules to disable.
     *
     * @return Rules.
     */
    protected abstract String[] disabledRules();

    protected <T extends RelNode> void assertPlan(
            TestCase testCase,
            Predicate<T> predicate,
            String... additionalRulesToDisable
    ) throws Exception {
        if (!missedCases.remove(testCase)) {
            fail("Testcase was as disabled: " + testCase);
        }

        String[] rulesToDisable = disabledRules();

        if (additionalRulesToDisable != null) {
            rulesToDisable = ArrayUtils.concat(rulesToDisable, additionalRulesToDisable);
        }

        assertPlan(testCase.query, Collections.singleton(testCase.schema), predicate, List.of(), rulesToDisable);
    }

    @SafeVarargs
    private static IgniteSchema schema(IgniteDistribution distribution,
            Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable>... indices) {
        org.apache.ignite.internal.sql.engine.framework.TestTable table = TestBuilders.table()
                .name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("VAL0", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.INT32)
                .addColumn("GRP0", NativeTypes.INT32)
                .addColumn("GRP1", NativeTypes.INT32)
                .size(DEFAULT_TBL_SIZE)
                .distribution(distribution)
                .build();

        for (Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable> index : indices) {
            index.accept(table);
        }

        IgniteSchema schema = new IgniteSchema("PUBLIC");
        schema.addTable(table);

        return schema;
    }

    private static IgniteSchema schemaWithAllNumerics(IgniteDistribution distribution) {
        org.apache.ignite.internal.sql.engine.framework.TestTable table = TestBuilders.table()
                .name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("GRP", NativeTypes.INT32)
                .addColumn("VAL_TINYINT", NativeTypes.INT8)
                .addColumn("VAL_SMALLINT", NativeTypes.INT16)
                .addColumn("VAL_INT", NativeTypes.INT32)
                .addColumn("VAL_BIGINT", NativeTypes.INT64)
                .addColumn("VAL_DECIMAL", NativeTypes.decimalOf(DecimalNativeType.DEFAULT_PRECISION, DecimalNativeType.DEFAULT_SCALE))
                .addColumn("VAL_FLOAT", NativeTypes.FLOAT)
                .addColumn("VAL_DOUBLE", NativeTypes.DOUBLE)
                .size(DEFAULT_TBL_SIZE)
                .distribution(distribution)
                .build();

        IgniteSchema schema = new IgniteSchema("PUBLIC");
        schema.addTable(table);

        return schema;
    }

    private static IgniteDistribution hash() {
        return IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID);
    }

    private static Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable> index(String name, int... cols) {
        RelCollation collation = TraitUtils.createCollation(IntStream.of(cols).boxed().collect(Collectors.toList()));

        return tbl -> tbl.addIndex(createIndex(tbl, name, collation));
    }

    private static Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable> indexByVal0Desc() {
        RelFieldCollation coll = new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING);

        return tbl -> tbl.addIndex(createIndex(tbl, "val0", RelCollations.of(coll)));
    }

    private static IgniteIndex createIndex(org.apache.ignite.internal.sql.engine.framework.TestTable tbl, String name,
            RelCollation collation) {
        return new IgniteIndex(TestSortedIndex.create(collation, name, tbl));
    }

    <T extends RelNode> Predicate<T> hasAggregate() {
        Predicate<T> mapNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class)
                .and(n -> n.getAggCallList().stream().anyMatch(NON_NULL_PREDICATE));
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class)
                .and(n -> n.getAggregateCalls().stream().anyMatch(NON_NULL_PREDICATE));

        return mapNode.or(reduceNode);
    }

    <T extends RelNode> Predicate<T> hasDistinctAggregate() {
        Predicate<T> mapNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class)
                .and(n -> n.getAggCallList().stream().anyMatch(NON_NULL_PREDICATE.and(AggregateCall::isDistinct)));
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class)
                .and(n -> n.getAggregateCalls().stream().anyMatch(NON_NULL_PREDICATE.and(AggregateCall::isDistinct)));

        return mapNode.or(reduceNode);
    }

    <T extends RelNode> Predicate<T> hasGroups() {
        Predicate<T> aggregateNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class).and(n -> !n.getGroupSets().isEmpty());
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class).and(n -> !n.getGroupSets().isEmpty());

        return aggregateNode.or(reduceNode);
    }
}
