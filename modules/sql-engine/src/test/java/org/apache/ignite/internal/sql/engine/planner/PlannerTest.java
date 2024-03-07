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
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlannerPhase;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * PlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class PlannerTest extends AbstractPlannerTest {
    private static List<String> NODES;

    private static List<NodeWithConsistencyToken> NODES_WITH_CONSISTENCY_TOKEN;

    /**
     * Init.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @BeforeAll
    public static void init() {
        NODES = new ArrayList<>(4);
        NODES_WITH_CONSISTENCY_TOKEN = new ArrayList<>(4);

        for (int i = 0; i < 4; i++) {
            String nodeName = Integer.toString(nextTableId());

            NODES.add(nodeName);
            NODES_WITH_CONSISTENCY_TOKEN.add(new NodeWithConsistencyToken(nodeName, 0L));
        }
    }

    @Test
    public void testMergeFilters() throws Exception {
        IgniteSchema publicSchema = createSchema(TestBuilders.table()
                .name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(IgniteDistributions.single())
                .build());

        String sql = "SELECT val from (\n"
                + "   SELECT * \n"
                + "   FROM TEST \n"
                + "   WHERE VAL = 10) \n"
                + "WHERE VAL = 10";

        assertPlan(sql, publicSchema, Predicate.not(nodeOrAnyChild(isInstanceOf(IgniteFilter.class)))
                .and(nodeOrAnyChild(isInstanceOf(IgniteTableScan.class)
                        .and(scan -> scan.condition() != null)
                )));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21286")
    public void testJoinPushExpressionRule() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                employerTable(IgniteDistributions.broadcast()),
                departmentTable(IgniteDistributions.broadcast())
        );

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "select d.deptno, e.deptno "
                + "from dept d, emp e "
                + "where d.deptno + e.deptno = 2";

        PlanningContext ctx = PlanningContext.builder()
                .parentContext(BaseQueryContext.builder()
                        .queryId(UUID.randomUUID())
                        .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schema)
                                .costFactory(new IgniteCostFactory(1, 100, 1, 1))
                                .build())
                        .build())
                .query(sql)
                .build();

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);

            String qry = ctx.query();

            assertNotNull(qry);

            // Parse
            SqlNode sqlNode = planner.parse(qry);

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            assertNotNull(rel);
            assertEquals("LogicalProject(DEPTNO=[$0], DEPTNO0=[$4])" + System.lineSeparator()
                            + "  LogicalFilter(condition=[=(+($0, $4), 2)])" + System.lineSeparator()
                            + "    LogicalJoin(condition=[true], joinType=[inner])" + System.lineSeparator()
                            + "      IgniteLogicalTableScan(table=[[PUBLIC, DEPT]])" + System.lineSeparator()
                            + "      IgniteLogicalTableScan(table=[[PUBLIC, EMP]])" + System.lineSeparator(),
                    RelOptUtil.toString(rel));

            // Transformation chain
            RelTraitSet desired = rel.getCluster().traitSet()
                    .replace(IgniteConvention.INSTANCE)
                    .replace(IgniteDistributions.single())
                    .simplify();

            IgniteRel phys = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            assertNotNull(phys);
            assertEquals(
                    "IgniteProject(DEPTNO=[$3], DEPTNO0=[$2])" + System.lineSeparator()
                            + "  IgniteCorrelatedNestedLoopJoin(condition=[=(+($3, $2), 2)], joinType=[inner], "
                            + "variablesSet=[[$cor2]])" + System.lineSeparator()
                            + "    IgniteTableScan(table=[[PUBLIC, EMP]])" + System.lineSeparator()
                            + "    IgniteTableScan(table=[[PUBLIC, DEPT]], filters=[=(+($t0, $cor2.DEPTNO), 2)])" + System.lineSeparator(),
                    RelOptUtil.toString(phys),
                    "Invalid plan:\n" + RelOptUtil.toString(phys)
            );

            checkSplitAndSerialization(phys, publicSchema);
        }
    }

    @Test
    public void testMergeJoinIsNotAppliedForNonEquiJoin() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                employerTable(IgniteDistributions.broadcast())
                        .andThen(setSize(1_000))
                        .andThen(addSortIndex("NAME", "DEPTNO")),
                departmentTable(IgniteDistributions.broadcast())
                        .andThen(setSize(100))
                        .andThen(addSortIndex("NAME", "DEPTNO"))
        );

        String sql = "select d.deptno, d.name, e.id, e.name from dept d join emp e "
                + "on d.deptno = e.deptno and e.name >= d.name order by e.name, d.deptno";

        RelNode phys = physicalPlan(sql, publicSchema, "CorrelatedNestedLoopJoin");

        assertNotNull(phys);
        assertEquals("Sort(sort0=[$3], sort1=[$0], dir0=[ASC], dir1=[ASC])" + System.lineSeparator()
                        + "  Project(DEPTNO=[$3], NAME=[$4], ID=[$0], NAME0=[$1])" + System.lineSeparator()
                        + "    NestedLoopJoin(condition=[AND(=($3, $2), >=($1, $4))], joinType=[inner])" + System.lineSeparator()
                        + "      TableScan(table=[[PUBLIC, EMP]])" + System.lineSeparator()
                        + "      TableScan(table=[[PUBLIC, DEPT]])" + System.lineSeparator(),
                RelOptUtil.toString(phys));
    }

    @Test
    public void testNotStandardFunctions() throws Exception {
        IgniteSchema publicSchema = createSchema(TestBuilders.table()
                .name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(IgniteDistributions.affinity(0, nextTableId(), DEFAULT_ZONE_ID))
                .build());

        String[] queries = {
                "select REVERSE(val) from TEST", // MYSQL
                "select DECODE(id, 0, val, '') from TEST" // ORACLE
        };

        for (String sql : queries) {
            IgniteRel phys = physicalPlan(
                    sql,
                    publicSchema
            );

            checkSplitAndSerialization(phys, publicSchema);
        }
    }

    @Test
    public void correctPlanningWithOrToUnion() throws Exception {
        IgniteSchema publicSchema = createSchema(TestBuilders.table()
                .name("TAB0")
                .addColumn("PK", NativeTypes.INT32)
                .addColumn("COL0", NativeTypes.INT32)
                .addColumn("COL1", NativeTypes.FLOAT)
                .addColumn("COL2", NativeTypes.STRING)
                .addColumn("COL3", NativeTypes.INT32)
                .addColumn("COL4", NativeTypes.FLOAT)
                .distribution(IgniteDistributions.affinity(0, nextTableId(), DEFAULT_ZONE_ID))
                .build());

        String sql = "SELECT pk FROM tab0 WHERE (((((col4 < 341.32))) AND col3 IN (SELECT col0 FROM tab0 WHERE ((col0 > 564) "
                + "AND col0 >= 344 AND (col0 IN (574) AND col0 > 600 AND ((col1 >= 568.71) AND col3 = 114 AND (col3 < 869))))) "
                + "OR col4 >= 811.8)) ORDER BY 1 DESC";

        // just check for planning completeness for finite time.
        assertPlan(sql, publicSchema, (k) -> true);
    }

    @Test
    public void checkTableHintsHandling() throws Exception {
        IgniteSchema publicSchema = createSchema(
                TestBuilders.table()
                        .name("PERSON")
                        .distribution(IgniteDistributions.affinity(0, nextTableId(), Integer.MIN_VALUE))
                        .addColumn("PK", NativeTypes.INT32)
                        .addColumn("ORG_ID", NativeTypes.INT32)
                        .build(),
                TestBuilders.table()
                        .name("COMPANY")
                        .distribution(IgniteDistributions.affinity(0, nextTableId(), Integer.MIN_VALUE))
                        .addColumn("PK", NativeTypes.INT32)
                        .addColumn("ID", NativeTypes.INT32)
                        .build()
        );

        String sql = "SELECT * FROM person /*+ use_index(ORG_ID), extra */ t1 JOIN company /*+ use_index(ID) */ t2 ON t1.org_id = t2.id";

        HintStrategyTable hintStrategies = HintStrategyTable.builder()
                .hintStrategy("use_index", (hint, rel) -> true)
                .hintStrategy("extra", (hint, rel) -> true)
                .build();

        Predicate<RelNode> hintCheck = nodeOrAnyChild(isInstanceOf(TableScan.class)
                .and(t -> "PERSON".equals(Util.last(t.getTable().getQualifiedName())))
                .and(t -> t.getHints().size() == 2)
        ).and(nodeOrAnyChild(isInstanceOf(TableScan.class)
                .and(t -> "COMPANY".equals(Util.last(t.getTable().getQualifiedName())))
                .and(t -> t.getHints().size() == 1)));

        assertPlan(sql, Collections.singleton(publicSchema), hintCheck, hintStrategies, List.of());
    }

    @Test
    public void testMinusDateSerialization() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema(DEFAULT_SCHEMA, 1, List.of());

        IgniteRel phys = physicalPlan("SELECT (DATE '2021-03-01' - DATE '2021-01-01') MONTHS", publicSchema);

        checkSplitAndSerialization(phys, publicSchema);
    }

    private static UnaryOperator<TableBuilder> employerTable(IgniteDistribution distribution) {
        return tableBuilder -> tableBuilder
                .name("EMP")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("NAME", NativeTypes.STRING)
                .addColumn("DEPTNO", NativeTypes.INT32)
                .size(100)
                .distribution(distribution);
    }

    private static UnaryOperator<TableBuilder> departmentTable(IgniteDistribution distribution) {
        return tableBuilder -> tableBuilder
                .name("DEPT")
                .addColumn("DEPTNO", NativeTypes.INT32)
                .addColumn("NAME", NativeTypes.STRING)
                .size(100)
                .distribution(distribution);
    }


}
