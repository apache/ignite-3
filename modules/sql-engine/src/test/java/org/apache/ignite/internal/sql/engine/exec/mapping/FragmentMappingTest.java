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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static java.util.UUID.randomUUID;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingTestRunner.TestSetup;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.IdentityDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests for fragment mapping.
 *
 * <p>Test setup:
 * <pre>
 *     // add nodes named n1, n2, and n3 to the cluster.
 *     addNodes("N1", "N2", "N3");
 *
 *     // The first statement adds a table with data located at node N1.
 *     // The second one adds a table with data located at nodes N2, N3.
 *     // The catalog is going to contain tables "T1_N1" and "T2_N2N3".
 *     // Both tables have distribution affinity([0], tableId, zoneId)
 *     addTable("T1", "N1");
 *     addTable("T2", "N2", "N3");
 *
 *     // Near semantic defines table T1 with primary partitions on nodes N0 and N1
 *     // together with backups placed on N1 and N2.
 *     addTable("T1", List.of(List.of("N0", "N1"), List.of("N1", "N2")));
 *
 *     // Adds table with identity(0) distribution. Can be used to mimic node system views.
 *     addTableIdent("NT1", "N1");
 *
 *     // Adds table with single distribution. Can be used to mimic cluster-wide system views.
 *     addTableSingle("CT2", "N1", "N2", "N3");
 *
 *     // Sets row count statistics to 100 for all tables named like T1_*
 *     setRowCount("T1", 100);
 *
 *     var runner = new MappingTestRunner("tests_dir");
 *     // run test cases
 *     runner.run(this::initSchema, "file.test");
 * </pre>
 *
 * <p>See {@link MappingTestRunner} for test file format description.
 *
 * @see MappingTestRunner
 */
public class FragmentMappingTest extends AbstractPlannerTest {

    private static final Path LOCATION = Paths.get("src/test/resources/mapping");

    private final TreeSet<String> nodeNames = new TreeSet<>();

    private final TreeMap<String, Pair<IgniteDistribution, List<List<String>>>> tables = new TreeMap<>();

    private final Map<String, Integer> tableRows = new HashMap<>();

    private final MappingTestRunner testRunner = new MappingTestRunner(LOCATION, this::parseQuery);

    @Test
    public void testValues() {
        addNodes("N1", "N2");

        testRunner.runTest(this::initSchema, "values.test");
    }

    @Test
    public void testTable() {
        addNodes("N1", "N2");

        addTable("T1", "N1");
        addTable("T2", "N1", "N2");

        testRunner.runTest(this::initSchema, "table_affinity.test");
    }

    @Test
    public void testDml() {
        addNodes("N0", "N1", "N2", "N3");

        addTable("T1", "N1");
        addTable("T2", "N2", "N3");

        testRunner.runTest(this::initSchema, "dml.test");
    }

    @Test
    public void testUnion() {
        addNodes("N1", "N2", "N3");

        addTable("T1", "N1");
        addTable("T1", "N1", "N2");

        addTable("T2", "N1");
        addTable("T2", "N1", "N2");

        testRunner.runTest(this::initSchema, "union.test");
    }

    @Test
    public void testSetOps() {
        addNodes("N1", "N2", "N3");

        addTable("T1", "N1");
        addTable("T2", "N2");

        testRunner.runTest(this::initSchema, "set_ops.test");
    }

    @Test
    public void testMergeJoin() {
        addNodes("N0", "N1", "N2", "N3", "N4");

        addTable("T1", "N1");
        addTable("T2", "N1");
        addTable("T2", "N2");

        setRowCount("T1", 200);
        setRowCount("T2", 100);

        testRunner.runTest(this::initSchema, "merge_join.test");
    }

    @Test
    public void testHashJoin() {
        addNodes("N0", "N1", "N2", "N3", "N4");

        addTable("T1", "N1");
        addTable("T2", "N1");
        addTable("T2", "N2");

        setRowCount("T1", 200);
        setRowCount("T2", 100);

        testRunner.runTest(this::initSchema, "hash_join.test");
    }

    @Test
    public void testTableIdentity() {
        addNodes("N0", "N1", "N2");

        addTableIdent("NT1", "N1");
        addTableIdent("NT2", "N1");
        addTableIdent("NT2", "N2");

        testRunner.runTest(this::initSchema, "table_identity.test");
    }

    @Test
    public void testTableSingleDistribution() {
        addNodes("N0", "N1", "N2");

        addTableSingle("CT1", "N1");
        addTableSingle("CT2", "N1");
        addTableSingle("CT2", "N2");

        testRunner.runTest(this::initSchema, "table_single.test");
    }

    @Test
    public void testTableSingleIdentityDistribution() {
        addNodes("N0", "N1", "N2");

        addTableSingle("CT", "N1");
        addTableSingle("CT", "N2");

        addTableIdent("NT", "N1");
        addTableIdent("NT", "N2");

        testRunner.runTest(this::initSchema, "table_identity_single.test");
    }

    @Test
    public void testCorrelated() {
        addNodes("N0", "N1", "N2");

        addTable("T", "N1");
        addTable("T", "N2");

        addTableSingle("CT", "N1");
        addTableSingle("CT", "N2");

        addTableIdent("NT", "N1");
        addTableIdent("NT", "N2");

        addTable("T1", "N0", "N1", "N2");
        addTable("T2", "N0", "N1", "N2");
        addTable("T3", "N0", "N1", "N2");

        testRunner.runTest(this::initSchema, "correlated.test");
    }

    @Test
    public void testTableFunctions() {
        addNodes("N0", "N1", "N2");

        addTable("T", "N1");

        addTableSingle("CT", "N1");
        addTableSingle("CT", "N2");

        addTableIdent("NT", "N1");
        addTableIdent("NT", "N2");

        testRunner.runTest(this::initSchema, "table_functions.test");
    }

    @Test
    public void testPartitionPruning() {
        addNodes("N0", "N1", "N2", "N3", "N4", "N5");

        addTable("T1", "N1", "N2", "N3");
        addTable("T2", "N4", "N5");
        addTable("T3", "N1", "N2", "N3");

        testRunner.runTest(this::initSchema, "test_partition_pruning.test");
    }

    @Test
    void backupPartitionsMapping() {
        addNodes("N0", "N1", "N2", "N3");

        addTable("T1", List.of(List.of("N0", "N1"), List.of("N1", "N2")));
        addTable("T2", List.of(List.of("N0", "N1", "N2")));
        addTable("T3", List.of(List.of("N1", "N2")));

        addTable("T4", List.of(List.of("N0", "N1"), List.of("N2", "N1")));

        testRunner.runTest(this::initSchema, "test_backup_mapping.test");
    }

    private void addNodes(String node, String... otherNodes) {
        this.nodeNames.add(node);
        this.nodeNames.addAll(Arrays.asList(otherNodes));
    }

    private void addTable(String name, String node, String... otherNodes) {
        TreeSet<String> nodeNames = new TreeSet<>();
        nodeNames.add(node);
        nodeNames.addAll(Arrays.asList(otherNodes));

        String tableName = formatName(name, nodeNames);
        tables.put(
                tableName,
                new Pair<>(
                        TestBuilders.affinity(-1, -1, -1),
                        nodeNames.stream()
                                .map(List::of)
                                .collect(Collectors.toList())
                )
        );
    }

    private void addTable(String name, List<List<String>> assignments) {
        String tableName = name;

        for (List<String> partitionNodes : assignments) {
            tableName = formatName(tableName, new LinkedHashSet<>(partitionNodes));
        }

        tables.put(
                tableName,
                new Pair<>(
                        TestBuilders.affinity(-1, -1, -1),
                        assignments
                )
        );
    }

    private void addTableSingle(String name, String... nodes) {
        TreeSet<String> nodeNames = new TreeSet<>(Arrays.asList(nodes));
        String tableName = formatName(name, nodeNames);

        tables.put(tableName, new Pair<>(IgniteDistributions.single(), List.of(new ArrayList<>(nodeNames))));
    }

    private void addTableIdent(String name, String node, String... otherNodes) {
        TreeSet<String> nodeNames = new TreeSet<>();
        nodeNames.add(node);
        nodeNames.addAll(Arrays.asList(otherNodes));

        String tableName = formatName(name, nodeNames);
        tables.put(
                tableName,
                new Pair<>(
                        IgniteDistributions.identity(0),
                        nodeNames.stream()
                                .map(List::of)
                                .collect(Collectors.toList())
                )
        );
    }

    private void setRowCount(String tableName, int rowCount) {
        this.tableRows.put(tableName, rowCount);
    }

    private IgniteRel parseQuery(IgniteSchema schema, String sqlStmt) {
        try {
            PlanningContext ctx = PlanningContext.builder()
                    .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                            .defaultSchema(createRootSchema(List.of(schema)).getSubSchema(schema.getName()))
                            .build())
                    .defaultSchemaName(schema.getName())
                    .query(sqlStmt)
                    .build();

            try (IgnitePlanner planner = ctx.planner()) {
                assertNotNull(planner);

                var plan = physicalPlan(planner, ctx.query());

                System.out.println(RelOptUtil.toString(plan, SqlExplainLevel.ALL_ATTRIBUTES));

                return physicalPlan(planner, ctx.query());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse a statement: " + sqlStmt, e);
        }
    }

    private TestSetup initSchema() {

        List<IgniteDataSource> dataSources = new ArrayList<>();
        int objectId = 1;

        Map<String, List<List<String>>> table2Assignments = new HashMap<>();

        for (Map.Entry<String, Pair<IgniteDistribution, List<List<String>>>> e : tables.entrySet()) {
            String tableName = e.getKey();
            String tableShortName = tableName.substring(0, tableName.indexOf('_'));
            // Generate distinct row counts for each table to ensure that the optimizer produces the same results.
            int tableSize = tableRows.getOrDefault(tableShortName, 100_000 + objectId);

            List<List<String>> assignments = e.getValue().getSecond();

            validateAssignments(tableShortName, assignments);

            IgniteDistribution distribution = e.getValue().getFirst();

            TableBuilder tableBuilder = TestBuilders.table().name(tableName);

            // To mimic node system views, id column is node name alias.
            if (distribution.function() instanceof IdentityDistribution) {
                tableBuilder = tableBuilder.addKeyColumn("ID", NativeTypes.stringOf(64));
            } else {
                // To mimic cluster wide system views
                tableBuilder = tableBuilder.addKeyColumn("ID", NativeTypes.INT32);
            }

            IgniteDistribution distributionToUse;
            if (distribution.isTableDistribution()) {
                distributionToUse = IgniteDistributions.affinity(
                        List.of(0),
                        objectId,
                        objectId,
                        TraitUtils.affinityDistributionLabel("PUBLIC", tableName, "ZONE_" + objectId)
                );
            } else {
                distributionToUse = distribution;
            }

            IgniteTable testTable = tableBuilder
                    .addColumn("C1", NativeTypes.INT32)
                    .addColumn("C2", NativeTypes.INT32)
                    .size(tableSize)
                    .tableId(objectId)
                    .distribution(distributionToUse)
                    .partitions(assignments.size())
                    .build();

            dataSources.add(testTable);

            table2Assignments.put(tableName, assignments);

            objectId += 1;
        }

        LogicalTopologySnapshot logicalTopologySnapshot = newLogicalTopology();

        IgniteSchema schema = new IgniteSchema(SqlCommon.DEFAULT_SCHEMA_NAME, 1, dataSources);
        ExecutionDistributionProvider executionDistributionProvider = TestBuilders.executionDistributionProviderBuilder()
                .useTablePartitions(true)
                .addTables(table2Assignments)
                .build();

        return new TestSetup(executionDistributionProvider, schema, logicalTopologySnapshot);
    }

    private void validateAssignments(String tableName, List<List<String>> assignments) {
        for (List<String> partitionAssignments : assignments) {
            for (String tableNodeName : partitionAssignments) {
                if (!nodeNames.contains(tableNodeName)) {
                    String message = format(
                            "Expected node {} for table {}. Registered nodes: {}",
                            tableNodeName, tableName, nodeNames
                    );
                    throw new IllegalArgumentException(message);
                }
            }
        }
    }

    private static String formatName(String name, Set<String> nodeNames) {
        return name + "_" + String.join("", nodeNames);
    }

    private LogicalTopologySnapshot newLogicalTopology() {
        List<LogicalNode> logicalNodes = nodeNames.stream()
                .map(name -> {
                    NetworkAddress addr = NetworkAddress.from("127.0.0.1:10000");
                    return new LogicalNode(randomUUID(), name, addr);
                })
                .collect(Collectors.toList());

        return new LogicalTopologySnapshot(1, logicalNodes);
    }
}
