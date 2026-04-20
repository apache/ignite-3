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

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests ProjectScanMergeRule and FilterScanMergeRule.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProjectFilterScanMergePlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /** {@inheritDoc} */
    @BeforeEach
    public void setup() {
        publicSchema = createSchemaFrom(createTestTable());
    }

    @Test
    public void testProjectFilterMerge() throws Exception {
        // Order of merge: ((scan + filter) + project).
        assertPlan("SELECT a, b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[$t0, $t1]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(0, 1, 2).equals(scan.requiredColumns()))
        );
    }

    @Test
    public void testIdentityFilterMerge() throws Exception {
        // Order of merge: ((scan + filter) + identity).
        assertPlan("SELECT a, b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(0, 1, 2).equals(scan.requiredColumns()))
        );
    }

    @Test
    public void testProjectFilterMergeSortedIndex() throws Exception {
        // Test project and filter merge into index scan.
        publicSchema = createSchemaFrom(createTestTable().andThen(addSortIndex("C")));

        // Without index condition shift.
        assertPlan("SELECT a, b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[$t0, $t1]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(0, 1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t2, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );

        // Index condition shifted according to requiredColumns.
        assertPlan("SELECT b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[$t0]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t1, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t1, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );
    }

    @Test
    public void testProjectFilterMergeHashIndex() throws Exception {
        // Test project and filter merge into index scan.
        publicSchema = createSchemaFrom(createTestTable().andThen(addHashIndex("C")));

        // Without index condition shift.
        assertPlan("SELECT a, b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[$t0, $t1]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(0, 1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t2, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );

        // Index condition shifted according to requiredColumns.
        assertPlan("SELECT b FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> "[$t0]".equals(scan.projects().toString()))
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t1, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t1, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );
    }

    @Test
    public void testIdentityFilterMergeIndex() throws Exception {
        // Test project and filter merge into index scan.
        publicSchema = createSchemaFrom(createTestTable().andThen(addSortIndex("C")));

        // Without index condition shift.
        assertPlan("SELECT a, b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(0, 1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t2, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );

        // Index condition shift and identity.
        assertPlan("SELECT b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t1, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t1, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );
    }

    @Test
    public void testIdentityFilterMergeHashIndex() throws Exception {
        // Test project and filter merge into index scan.
        publicSchema = createSchemaFrom(createTestTable().andThen(addHashIndex("C")));

        // Without index condition shift.
        assertPlan("SELECT a, b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t2, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(0, 1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t2, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );

        // Index condition shift and identity.
        assertPlan("SELECT b, c FROM tbl WHERE c = 0", publicSchema, isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.projects() == null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t1, 0)".equals(scan.condition().toString()))
                .and(scan -> ImmutableIntList.of(1, 2).equals(scan.requiredColumns()))
                .and(scan -> "[=($t1, 0)]".equals(searchBoundsCondition(scan.searchBounds()).toString()))
        );
    }

    @Test
    public void testProjectFilterProjectMerge() throws Exception {
        // Inner query contains correlate, it prevents filter to be moved below project, and after HEP_FILTER_PUSH_DOWN
        // phase we should have chain Project - Filter - Project - Scan. Whole this chain should be merged into a single
        // table scan on the next phases. Order of merge: (((scan + inner project) + filter) + outer project).
        String sql = "SELECT /*+ disable_decorrelation, DISABLE_RULE('ExposeIndexRule') */(SELECT a+2 FROM (SELECT c, a+1 AS a FROM tbl) "
                + "AS t2 WHERE t2.c = t1.c) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() != null)
                        .and(scan -> "[+(+($t1, 1), 2)]".equals(scan.projects().toString()))
                        .and(scan -> scan.condition() != null)
                        .and(scan -> "=($t0, $cor0.C)".equals(scan.condition().toString()))
                        .and(scan -> ImmutableIntList.of(2, 0).equals(scan.requiredColumns()))
                ))));
    }

    @Test
    public void testIdentityFilterProjectMerge() throws Exception {
        // The same as two projects merge, but outer project is identity and should be eliminated together with inner
        // project by project to scan merge rule.
        String sql = "SELECT /*+ disable_decorrelation */ (SELECT a FROM (SELECT a, a+1 FROM tbl) AS t2 WHERE t2.a = t1.a) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() == null)
                        .and(scan -> scan.condition() != null)
                        .and(scan -> "=($t0, $cor0.A)".equals(scan.condition().toString()))
                        .and(scan -> ImmutableIntList.of(0).equals(scan.requiredColumns()))
                ))), "ProjectFilterTransposeRule");

        // Filter on project that is not permutation should be merged too.
        sql = "SELECT /*+ disable_decorrelation */ (SELECT a FROM (SELECT a+1 AS a FROM tbl) AS t2 WHERE t2.a = t1.a) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() != null)
                        .and(scan -> "[+($t0, 1)]".equals(scan.projects().toString()))
                        .and(scan -> scan.condition() != null)
                        .and(scan -> "=(+($t0, 1), $cor0.A)".equals(scan.condition().toString()))
                        .and(scan -> ImmutableIntList.of(0).equals(scan.requiredColumns()))
                ))), "ProjectFilterTransposeRule");
    }

    @Test
    public void testProjectFilterIdentityMerge() throws Exception {
        // The same as two projects merge, but inner project is identity and should be eliminated by project to scan
        // merge rule.
        String sql = "SELECT /*+ disable_decorrelation */ (SELECT a+2 FROM (SELECT a, c FROM tbl) AS t2 WHERE t2.c = t1.c) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() != null)
                        .and(scan -> "[+($t0, 2)]".equals(scan.projects().toString()))
                        .and(scan -> scan.condition() != null)
                        .and(scan -> "=($t1, $cor0.C)".equals(scan.condition().toString()))
                        .and(scan -> ImmutableIntList.of(0, 2).equals(scan.requiredColumns()))
                ))));
    }

    @Test
    public void testIdentityFilterIdentityMerge() throws Exception {
        // The same as two projects merge, but projects are identity and should be eliminated by project to scan
        // merge rule.
        String sql = "SELECT /*+ disable_decorrelation */ (SELECT c FROM (SELECT a AS c FROM tbl) AS t2 WHERE t2.c = t1.c) FROM tbl AS t1";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() == null)
                        .and(scan -> scan.condition() != null)
                        .and(scan -> "=($t0, $cor0.C)".equals(scan.condition().toString()))
                        .and(scan -> ImmutableIntList.of(0).equals(scan.requiredColumns()))
                ))));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18688")
    public void testFilterProjectFilterMerge() throws Exception {
        String sql = "SELECT * FROM (SELECT c, a FROM tbl WHERE a = 1) WHERE c = 1";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() != null)
                        .and(scan -> "[$t1, $t0]".equals(scan.projects().toString()))
                        .and(scan -> scan.condition() != null)
                        .and(scan -> "AND(=($t0, 1), =($t1, 1))".equals(scan.condition().toString()))
                        .and(scan -> ImmutableIntList.of(0, 2).equals(scan.requiredColumns())),
                "ProjectFilterTransposeRule", "FilterProjectTransposeRule");
    }

    @Test
    public void testFilterIdentityFilterMerge() throws Exception {
        String sql = "SELECT * FROM (SELECT a, c FROM tbl WHERE a = 1) WHERE c = 1";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() == null)
                        .and(scan -> scan.condition() != null)
                        .and(scan -> "AND(=($t0, 1), =($t1, 1))".equals(scan.condition().toString()))
                        .and(scan -> ImmutableIntList.of(0, 2).equals(scan.requiredColumns())),
                "ProjectFilterTransposeRule", "FilterProjectTransposeRule");
    }

    @Test
    public void testAlwaysTrueFilterPruning() throws Exception {
        String sql = "SELECT a, c FROM tbl WHERE a > 1 OR a < 3 OR a IS NULL";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.projects() == null)
                        .and(scan -> scan.condition() == null)
                        .and(scan -> ImmutableIntList.of(0, 2).equals(scan.requiredColumns())),
                "ProjectFilterTransposeRule", "FilterProjectTransposeRule");
    }

    @Test
    public void testAlwaysFalseFilterPruning() throws Exception {
        Predicate<IgniteValues> hasEmptyValuesOnly = hasEmptyValuesOnlyPredicate();

        // Table scan elimination.
        String sql = "SELECT a, c FROM tbl WHERE a > 1 AND a < 0";
        assertPlan(sql, publicSchema, hasEmptyValuesOnly);

        sql = "SELECT a, c FROM (SELECT a, c FROM tbl WHERE a > 1) WHERE c = 1 AND c IS NULL";
        assertPlan(sql, publicSchema, hasEmptyValuesOnly,
                "ProjectFilterTransposeRule", "FilterProjectTransposeRule");

        sql = "SELECT a, c FROM (SELECT a, c FROM tbl WHERE a > 1) WHERE a < 0";
        assertPlan(sql, publicSchema, hasEmptyValuesOnly,
                "ProjectFilterTransposeRule", "FilterProjectTransposeRule");

        // JOIN branch elimination.
        sql = "SELECT t1.a, t2.a, t1.c FROM tbl AS t1 LEFT JOIN tbl AS t2 ON t1.a = t2.a WHERE t2.a = 1 AND t2.a IS NULL AND t1.c = 1";
        assertPlan(sql, publicSchema, hasEmptyValuesOnly);

        sql = "SELECT t1.a, t2.a, t1.c FROM tbl AS t1 INNER JOIN tbl AS t2 ON t1.a = t2.a WHERE t2.a = 1 AND t2.a IS NULL";
        assertPlan(sql, publicSchema, hasEmptyValuesOnly);

        sql = "SELECT t1.a, t2.a, t1.c FROM tbl AS t1 INNER JOIN tbl AS t2 ON t1.a = t2.a WHERE t1.a = 1 AND t2.a = 2";
        assertPlan(sql, publicSchema, hasEmptyValuesOnlyPredicate());
    }

    @Test
    public void testJoinWithAlwaysFalseConditionPruning() throws Exception {
        String sql = "SELECT t1.a, t2.a, t1.c FROM tbl AS t1 LEFT JOIN tbl AS t2 ON (t1.a = t2.a AND t2.a = 1 AND t2.a = 2) WHERE t1.c = 1";
        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> scan.condition() != null)
                .and(scan -> "=($t1, 1)".equals(scan.condition().toString()))
        );

        sql = "SELECT t1.a, t2.a, t1.c FROM tbl AS t1 INNER JOIN tbl AS t2 ON t1.a = t2.a AND t2.a = 1 AND t2.a = 2";
        assertPlan(sql, publicSchema, hasEmptyValuesOnlyPredicate());
    }

    @Test
    public void testAlwaysFalseFilterPruningWithDml() throws Exception {
        Predicate<IgniteValues> zeroDmlResultPredicate = isInstanceOf(IgniteValues.class)
                .and(values -> values.getTuples().size() == 1) // single row
                .and(values -> values.getTuples().get(0).size() == 1) // row of single column
                .and(values -> RexLiteral.longValue(values.getTuples().get(0).get(0)) == 0L);

        String sql = "INSERT INTO tbl (a, c) SELECT a, b FROM tbl WHERE a > 1 AND a < 0";
        assertPlan(sql, publicSchema, zeroDmlResultPredicate);

        sql = "INSERT INTO tbl (a, c) (SELECT a, c FROM (SELECT a, c FROM tbl WHERE a > 1) WHERE a < 0)";
        assertPlan(sql, publicSchema, zeroDmlResultPredicate);
    }

    private Predicate<IgniteValues> hasEmptyValuesOnlyPredicate() {
        return isInstanceOf(IgniteValues.class).and(values -> values.getTuples().isEmpty());
    }

    /**
     * Convert search bounds to RexNodes.
     */
    private static List<RexNode> searchBoundsCondition(List<SearchBounds> searchBounds) {
        return searchBounds.stream().filter(Objects::nonNull).map(SearchBounds::condition).collect(Collectors.toList());
    }

    private static UnaryOperator<TableBuilder> createTestTable() {
        return t -> t.name("TBL")
                .addColumn("A", NativeTypes.INT32)
                .addColumn("B", NativeTypes.INT32)
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single());
    }
}
