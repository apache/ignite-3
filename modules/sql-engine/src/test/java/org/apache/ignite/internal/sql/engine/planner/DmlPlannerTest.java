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

import static org.apache.ignite.internal.sql.engine.util.Commons.cast;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify multi-step versions of DML plans.
 */
@WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "false")
public class DmlPlannerTest extends AbstractPlannerTest {
    @BeforeEach
    @AfterEach
    public void resetFlag() {
        Commons.resetFastQueryOptimizationFlag();
    }

    /**
     * Test for INSERT .. VALUES when table has a single distribution.
     */
    @Test
    public void testInsertIntoSingleDistributedTable() throws Exception {
        IgniteTable test1 = newTestTable("TEST1", IgniteDistributions.single());
        IgniteSchema schema = createSchema(test1);

        // There should be no exchanges and other operations.
        assertPlan("INSERT INTO TEST1 (C1, C2) VALUES(1, 2)", schema,
                isInstanceOf(IgniteTableModify.class).and(input(isInstanceOf(IgniteValues.class))),
                DISABLE_KEY_VALUE_MODIFY_RULES
        );
    }

    /**
     * Test for INSERT .. VALUES when table has non single distribution.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testInsert(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);

        IgniteSchema schema = createSchema(test1);

        assertPlan("INSERT INTO TEST1 (C1, C2) VALUES(1, 2)", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteTrimExchange.class).and(e -> distribution.equals(e.distribution()))))),
                DISABLE_KEY_VALUE_MODIFY_RULES
        );
    }

    private static Stream<IgniteDistribution> nonSingleDistributions() {
        return distributions().filter(d -> !IgniteDistributions.single().equals(d));
    }

    /**
     * Test for INSERT .. FROM SELECT when tables has different distributions.
     */
    @ParameterizedTest
    @MethodSource("distributions")
    public void testInsertSelectFrom(IgniteDistribution distribution) throws Exception {
        IgniteDistribution anotherDistribution = TestBuilders.affinity(1, 1, 0);

        IgniteTable test1 = newTestTable("TEST1", distribution);
        IgniteTable test2 = newTestTable("TEST2", anotherDistribution);

        IgniteSchema schema = createSchema(test1, test2);

        assertPlan("INSERT INTO TEST1 (C1, C2) SELECT C1, C2 FROM TEST2", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteExchange.class).and(e -> distribution.equals(e.distribution())))))
        );
    }

    /**
     * Test for INSERT .. FROM SELECT when tables has the same distribution.
     */
    @ParameterizedTest
    @MethodSource("distributions")
    public void testInsertSelectFromSameDistribution(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);
        IgniteTable test2 = newTestTable("TEST2", distribution);

        IgniteSchema schema = createSchema(test1, test2);

        // there should be no exchanges.
        assertPlan("INSERT INTO TEST1 (C1, C2) SELECT C1, C2 FROM TEST2", schema,
                nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                        .and(hasChildThat(isInstanceOf(IgniteTableScan.class)))
        );
    }

    /**
     * Test for UPDATE when table has a single distribution.
     */
    @Test
    public void testUpdateOfSingleDistributedTable() throws Exception {
        IgniteTable test1 = newTestTable("TEST1", IgniteDistributions.single());
        IgniteSchema schema = createSchema(test1);

        // There should be no exchanges and other operations.
        assertPlan("UPDATE TEST1 SET C2 = C2 + 1", schema,
                isInstanceOf(IgniteTableModify.class).and(input(isInstanceOf(IgniteTableScan.class))));
    }

    /**
     * Test for UPDATE when table has non single distribution.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testUpdate(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);

        IgniteSchema schema = createSchema(test1);

        assertPlan("UPDATE TEST1 SET C2 = C2 + 1", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteTableScan.class))))
        );
    }

    @ParameterizedTest
    @MethodSource("distributionsForDelete")
    public void testDelete(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = TestBuilders.table()
                .name("TEST1")
                .addColumn("C1", NativeTypes.INT32)
                .addKeyColumn("KEY1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .addKeyColumn("KEY2", NativeTypes.INT32)
                .distribution(distribution)
                .build();

        IgniteSchema schema = createSchema(test1);

        // There should be no exchange between the modify node and the scan node.
        assertPlan("DELETE FROM TEST1 WHERE KEY1 = 1 and KEY2 = 2", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class)
                                .and(input(isTableScan("TEST1")))))
        );
    }

    private static Stream<IgniteDistribution> distributions() {
        return Stream.of(
                IgniteDistributions.single(),
                IgniteDistributions.hash(List.of(0, 1)),
                TestBuilders.affinity(0, 2, 0),
                IgniteDistributions.identity(0)
        );
    }

    /**
     * Creates a list of non-single distributions with keys corresponding to the indexes of the key columns of the table.
     *
     * @return Distributions to test DELETE operation.
     */
    private static Stream<IgniteDistribution> distributionsForDelete() {
        return Stream.of(
                IgniteDistributions.hash(List.of(1, 3)),
                TestBuilders.affinity(1, 2, 0),
                TestBuilders.affinity(3, 2, 0),
                TestBuilders.affinity(List.of(1, 3), 2, 0),
                TestBuilders.affinity(List.of(3, 1), 2, 0),
                IgniteDistributions.identity(1)
        );
    }

    /**
     * Test for check basic dml operators when table doesn't exist.
     */
    @ParameterizedTest
    @MethodSource("basicStatements")
    public void testDmlQueriesOnNonExistingTable(String query) {
        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrowsWithCause(
                () -> physicalPlan(query, createSchema(newTestTable("TEST", IgniteDistributions.single()))),
                SqlValidatorException.class,
                "Object 'UNKNOWN_T' not found"
        );
    }

    private static Stream<String> basicStatements() {
        return Stream.of(
                "SELECT * FROM unknown_t",
                "INSERT INTO unknown_t VALUES(1)",
                "UPDATE unknown_t SET ID=1",
                "DELETE FROM unknown_t",
                "MERGE INTO unknown_t DST USING test SRC ON DST.C1 = SRC.C1"
                        + " WHEN MATCHED THEN UPDATE SET C2 = SRC.C2"
                        + " WHEN NOT MATCHED THEN INSERT (C1, C2) VALUES (SRC.C1, SRC.C2)",
                "MERGE INTO test DST USING unknown_t SRC ON DST.C1 = SRC.C1"
                        + " WHEN MATCHED THEN UPDATE SET C2 = SRC.C2"
                        + " WHEN NOT MATCHED THEN INSERT (C1, C2) VALUES (SRC.C1, SRC.C2)"
        );
    }

    /**
     * Tests that primary key columns are not modifiable.
     */
    @ParameterizedTest
    @MethodSource("updatePrimaryKey")
    public void testDoNotAllowToModifyPrimaryKeyColumns(String query) {
        IgniteTable test = TestBuilders.table()
                .name("TEST")
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = createSchema(test);

        IgniteTestUtils.assertThrowsWithCause(
                () -> physicalPlan(query, schema),
                SqlValidatorException.class,
                "Primary key columns are not modifiable"
        );
    }

    @Test
    @WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "true")
    public void testValuesNodeTypeDerivationForDefaultOperator() throws Exception {
        IgniteTable test = TestBuilders.table()
                .name("TEST")
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("UUID_VAL", NativeTypes.UUID, new UUID(1L, 2L))
                .addColumn("INT_VAL", NativeTypes.INT32, 42)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = createSchema(test);

        Predicate<List<RexNode>> expressionsAsOfExpectedType =
                expressions -> expressionsAsOfType(expressions, NativeTypes.INT32, NativeTypes.UUID, NativeTypes.INT32);

        Predicate<IgniteKeyValueModify> kvModifyNodeWithExpressionsOfExpectedTypes = isInstanceOf(IgniteKeyValueModify.class)
                .and(kvModify -> expressionsAsOfExpectedType.test(kvModify.expressions()));

        assertPlan(
                "INSERT INTO test (id, int_val) VALUES (1, DEFAULT)",
                schema,
                kvModifyNodeWithExpressionsOfExpectedTypes
        );

        assertPlan(
                "INSERT INTO test (id, uuid_val) VALUES (1, DEFAULT)",
                schema,
                kvModifyNodeWithExpressionsOfExpectedTypes
        );

        Predicate<IgniteValues> valuesNodeWithProjectionsOfExpectedTypes = isInstanceOf(IgniteValues.class)
                .and(project -> expressionsAsOfExpectedType.test(cast(project.getTuples().get(0))));
        Predicate<IgniteProject> projectNodeWithProjectionsOfExpectedTypes = isInstanceOf(IgniteProject.class)
                .and(project -> expressionsAsOfExpectedType.test(project.getProjects()));

        assertPlan(
                "INSERT INTO test VALUES (1, DEFAULT, 1), (1, 'asd'::UUID, DEFAULT)",
                schema,
                hasChildThat(
                        isInstanceOf(IgniteUnionAll.class)
                                .and(input(0, valuesNodeWithProjectionsOfExpectedTypes))
                                .and(input(1, projectNodeWithProjectionsOfExpectedTypes))
                )
        );
    }

    @Test
    public void testMergeWithSubquery() throws Exception {
        IgniteTable test1 = TestBuilders.table()
                .name("T1")
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.INT32)
                .addColumn("VAL2", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteTable test2 = TestBuilders.table()
                .name("T2")
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.INT32)
                .addColumn("VAL2", NativeTypes.INT32)
                .addColumn("VAL3", NativeTypes.INT32)
                .addColumn("VAL4", NativeTypes.INT32)
                .addColumn("VAL5", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = createSchema(test1, test2);

        assertPlan(
                "MERGE INTO t1 dst\n"
                        + " USING (\n"
                        + "    SELECT t1.id, t2.val5\n"
                        + "      FROM t1 LEFT JOIN t2 ON t1.id = t2.id\n"
                        + " ) src\n"
                        + "   ON src.id = dst.id\n"
                        + " WHEN MATCHED THEN UPDATE SET val1 = src.val5\n"
                        + " WHEN NOT MATCHED THEN INSERT (id, val1) VALUES (src.id, src.val5)",
                schema,
                isInstanceOf(IgniteTableModify.class)
                        .and(hasChildThat(isInstanceOf(IgniteProject.class).and(
                                expectedProject(
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 3,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 4,
                                        p -> p instanceof RexLiteral && ((RexLiteral) p).isNull(),
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 0,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 1,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 2,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 4
                                ))))
                        .and(hasChildThat(isInstanceOf(IgniteHashJoin.class)
                                .and(m -> m.getRowType().getFieldNames().equals(List.of("ID", "VAL1", "VAL2", "ID$0", "VAL5")))
                        )));

        assertPlan(
                "MERGE INTO t1 dst\n"
                        + " USING (\n"
                        + "    SELECT t1.id, t2.val5\n"
                        + "      FROM t1 LEFT JOIN t2 ON t1.id = t2.id\n"
                        + " ) src\n"
                        + "   ON src.id = dst.id\n"
                        + " WHEN MATCHED THEN UPDATE SET val1 = src.val5\n"
                        + " WHEN NOT MATCHED THEN INSERT (id, val2) VALUES (src.id, src.val5)",
                schema,
                isInstanceOf(IgniteTableModify.class)
                        .and(hasChildThat(isInstanceOf(IgniteProject.class).and(
                                expectedProject(
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 3,
                                        p -> p instanceof RexLiteral && ((RexLiteral) p).isNull(),
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 4,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 0,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 1,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 2,
                                        p -> p instanceof RexInputRef && ((RexInputRef) p).getIndex() == 4
                                ))))
                        .and(hasChildThat(isInstanceOf(IgniteHashJoin.class)
                                .and(m -> m.getRowType().getFieldNames().equals(List.of("ID", "VAL1", "VAL2", "ID$0", "VAL5")))
                        )));
    }

    @SafeVarargs
    private static Predicate<IgniteProject> expectedProject(Predicate<RexNode>... predicates) {
        return p -> {
            int i = 0;
            assertEquals(p.getProjects().size(), predicates.length);
            for (RexNode project : p.getProjects()) {
                if (!predicates[i++].test(project)) {
                    return false;
                }
            }
            return true;
        };
    }

    private static Stream<String> updatePrimaryKey() {
        return Stream.of(
                "UPDATE TEST SET ID = ID + 1",
                "MERGE INTO test DST USING test SRC ON DST.VAL = SRC.VAL"
                        + " WHEN MATCHED THEN UPDATE SET ID = SRC.ID + 1"
        );
    }

    // Class name is fully-qualified because AbstractPlannerTest defines a class with the same name.
    private static IgniteTable newTestTable(String tableName, IgniteDistribution distribution) {
        return TestBuilders.table()
                .name(tableName)
                .addColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(distribution)
                .build();
    }

    private static boolean expressionsAsOfType(List<RexNode> expressions, NativeType... expectedTypes) {
        assert expressions.size() == expectedTypes.length;

        for (int i = 0; i < expressions.size(); i++) {
            RelDataType actualType = expressions.get(i).getType();
            RelDataType expectedType = TypeUtils.native2relationalType(Commons.typeFactory(), expectedTypes[i], actualType.isNullable());

            if (actualType != expectedType) {
                return false;
            }
        }

        return true;
    }
}
