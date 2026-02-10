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

import static org.apache.ignite.internal.sql.engine.planner.CorrelatedSubqueryPlannerTest.createTestTable;
import static org.apache.ignite.internal.sql.engine.planner.JoinColocationPlannerTest.complexTbl;
import static org.apache.ignite.internal.sql.engine.planner.JoinColocationPlannerTest.simpleTable;
import static org.apache.ignite.internal.sql.engine.planner.JoinColocationPlannerTest.simpleTableHashPk;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** HashJoin planner test. */
public class HashJoinPlannerTest extends AbstractPlannerTest {
    private static final String[] disabledRules = {"NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "MergeJoinConverter"};

    private static final String[] joinTypes = {"LEFT", "RIGHT", "INNER", "FULL OUTER"};

    /**
     * Hash join need to preserve left collation.
     */
    @Test
    public void hashJoinCheckLeftCollationsPropagation() throws Exception {
        IgniteTable tbl1 = simpleTable("TEST_TBL", DEFAULT_TBL_SIZE);
        IgniteTable tbl2 = complexTbl("TEST_TBL_CMPLX");

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
                + "from TEST_TBL_CMPLX t2 "
                + "join TEST_TBL t1 on t1.id = t2.id1 "
                + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        // Only hash join
        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin", "MergeJoinConverter", "JoinCommuteRule");

        IgniteHashJoin join = findFirstNode(phys, byClass(IgniteHashJoin.class));
        List<RelNode> joinNodes = findNodes(phys, byClass(IgniteHashJoin.class));
        List<RelNode> sortNodes = findNodes(phys, byClass(IgniteSort.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, sortNodes.size(), equalTo(0));
        assertThat(invalidPlanMsg, joinNodes.size(), equalTo(1));
        assertThat(invalidPlanMsg, join, notNullValue());
    }

    /**
     * Hash join erase right collation.
     */
    @Test
    public void hashJoinCheckRightCollations() throws Exception {
        IgniteTable tbl1 = simpleTable("TEST_TBL", DEFAULT_TBL_SIZE);
        IgniteTable tbl2 = complexTbl("TEST_TBL_CMPLX");

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
                + "from TEST_TBL t1 "
                + "join TEST_TBL_CMPLX t2 on t1.id = t2.id1 "
                + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        // Only hash join
        IgniteRel phys = physicalPlan(sql, schema, "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin", "MergeJoinConverter", "JoinCommuteRule");

        IgniteHashJoin join = findFirstNode(phys, byClass(IgniteHashJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, sortOnTopOfJoin(phys), notNullValue());
    }

    @Test
    public void hashJoinWinsOnLeftSkewedInput() throws Exception {
        IgniteTable thinTblSortedPk = simpleTable("THIN_TBL", 10);
        IgniteTable thinTblHashPk = simpleTableHashPk("THIN_TBL_HASH_PK", 10);
        IgniteTable thickTblSortedPk = simpleTable("THICK_TBL", 100_000);
        IgniteTable thickTblHashPk = simpleTableHashPk("THICK_TBL_HASH_PK", 100_000);

        IgniteSchema schema = createSchema(thinTblSortedPk, thickTblSortedPk, thinTblHashPk, thickTblHashPk);

        String sql = "select t1.ID, t1.ID2, t2.ID, t2.ID2 "
                + "from THICK_TBL t1 " // left
                + "join THIN_TBL t2 on t1.ID2 = t2.ID2 "; // right

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)), "JoinCommuteRule");

        sql = "select t1.ID, t1.ID2, t2.ID, t2.ID2 "
                + "from THIN_TBL t1 " // left
                + "join THICK_TBL t2 on t1.ID2 = t2.ID2 "; // right

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class).negate()), "JoinCommuteRule");

        sql = "select t1.ID, t1.ID2, t2.ID, t2.ID2 "
                + "from THIN_TBL_HASH_PK t1 " // left
                + "join THICK_TBL_HASH_PK t2 on t1.ID = t2.ID "; // right

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)), "JoinCommuteRule");

        // merge join can consume less cpu in such a case
        sql = "select t1.ID, t1.ID2, t2.ID, t2.ID2 "
                + "from THIN_TBL t1 " // left
                + "join THICK_TBL t2 on t1.ID = t2.ID "; // right

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class).negate()), "JoinCommuteRule");
    }

    private static @Nullable IgniteSort sortOnTopOfJoin(IgniteRel root) {
        List<IgniteSort> sortNodes = findNodes(root, byClass(IgniteSort.class)
                .and(node -> node.getInputs().size() == 1 && node.getInput(0) instanceof Join));

        if (sortNodes.size() > 1) {
            throw new AssertionError("Unexpected count of sort nodes: exp<=1, act=" + sortNodes.size());
        }

        return sortNodes.isEmpty() ? null : sortNodes.get(0);
    }

    /** Check that only appropriate conditions are acceptable for hash join. */
    @ParameterizedTest
    @MethodSource("joinConditions")
    @SuppressWarnings("ThrowableNotThrown")
    public void hashJoinAppliedConditions(String sql, boolean canBePlanned, boolean skipIfRightOrFull) throws Exception {
        IgniteTable tbl = createTestTable("ID", "C1");

        IgniteSchema schema = createSchema(tbl);

        for (String type : joinTypes) {
            if (skipIfRightOrFull && !Set.of("INNER", "SEMI", "LEFT").contains(type)) {
                continue;
            }

            String sql0 = String.format(sql, type);

            if (canBePlanned) {
                assertPlan(sql0, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)), disabledRules);
            } else {
                IgniteTestUtils.assertThrowsWithCause(() -> physicalPlan(sql0, schema, disabledRules),
                        CannotPlanException.class,
                        "There are not enough rules");
            }
        }
    }

    private static Stream<Arguments> joinConditions() {
        return Stream.of(
                Arguments.of("select t1.c1 from t1 %s join t1 t2 on t1.c1 = t2.c1", true, false),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 using(c1)", true, false),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 on t1.c1 = 1", false, false),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 ON t1.id is not distinct from t2.c1", false, false),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 on t1.c1 = ?", false, false),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 on t1.c1 = OCTET_LENGTH('TEST')", false, false),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 on t1.c1 = LOG10(t1.c1)", false, false),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 on t1.c1 = t2.c1 and t1.ID > t2.ID", true, true),
                Arguments.of("select t1.c1 from t1 %s join t1 t2 on t1.c1 = 1 and t2.c1 = 1", false, false)
        );
    }
}
