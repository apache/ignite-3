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

package org.apache.ignite.internal.sql.engine.prepare.pruning;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.generateLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Tests for {@link PartitionPruningPredicate}.
 */
public class PartitionPruningPredicateSelfTest extends BaseIgniteAbstractTest {

    private static final long SOURCE_ID = ThreadLocalRandom.current().nextLong();

    private final long seed = System.nanoTime();

    @BeforeEach
    public void beforeEach() {
        log.info("Seed is {}", seed);
    }

    private static List<NativeType> nativeTypes() {
        List<NativeType> nativeTypes = new ArrayList<>();
        nativeTypes.add(NativeTypes.BOOLEAN);
        nativeTypes.add(NativeTypes.INT8);
        nativeTypes.add(NativeTypes.INT16);
        nativeTypes.add(NativeTypes.INT32);
        nativeTypes.add(NativeTypes.INT64);
        nativeTypes.add(NativeTypes.FLOAT);
        nativeTypes.add(NativeTypes.DOUBLE);

        nativeTypes.add(NativeTypes.decimalOf(2, 0));
        nativeTypes.add(NativeTypes.decimalOf(6, 0));
        nativeTypes.add(NativeTypes.decimalOf(12, 12));
        nativeTypes.add(NativeTypes.decimalOf(16, 8));
        nativeTypes.add(NativeTypes.decimalOf(42, 37));

        nativeTypes.add(NativeTypes.stringOf(16));
        nativeTypes.add(NativeTypes.blobOf(16));

        nativeTypes.add(NativeTypes.DATE);

        nativeTypes.add(NativeTypes.time(0));
        nativeTypes.add(NativeTypes.time(3));
        nativeTypes.add(NativeTypes.time(5));
        nativeTypes.add(NativeTypes.time(6));
        nativeTypes.add(NativeTypes.time(8));
        nativeTypes.add(NativeTypes.time(9));

        nativeTypes.add(NativeTypes.timestamp(0));
        nativeTypes.add(NativeTypes.timestamp(3));
        nativeTypes.add(NativeTypes.timestamp(5));
        nativeTypes.add(NativeTypes.timestamp(6));
        nativeTypes.add(NativeTypes.timestamp(8));
        nativeTypes.add(NativeTypes.timestamp(9));

        nativeTypes.add(NativeTypes.datetime(0));
        nativeTypes.add(NativeTypes.datetime(3));
        nativeTypes.add(NativeTypes.datetime(5));
        nativeTypes.add(NativeTypes.datetime(6));
        nativeTypes.add(NativeTypes.datetime(8));
        nativeTypes.add(NativeTypes.datetime(9));

        nativeTypes.add(NativeTypes.UUID);

        return nativeTypes;
    }

    @ParameterizedTest
    @MethodSource("nativeTypes")
    public void testLiteralValue(NativeType nativeType) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-19162 Ignite doesn't support precision more than 3 for temporal types.
        if (nativeType instanceof TemporalNativeType) {
            TemporalNativeType temporalNativeType = (TemporalNativeType) nativeType;
            Assumptions.assumeFalse(temporalNativeType.precision() > 3);
        }

        IgniteDistribution distribution = TestBuilders.affinity(List.of(0), 1, 1);

        IgniteTable table = TestBuilders.table()
                .name("T")
                .partitions(5)
                .addKeyColumn("c1", nativeType)
                .addColumn("c2", NativeTypes.INT32)
                .distribution(distribution)
                .build();

        int fieldIndex = 0;
        Object val = generateFieldValue(table, fieldIndex, nativeType);
        RexNode expr = generateLiteral(nativeType.spec(), val);

        PartitionPruningColumns columns = new PartitionPruningColumns(List.of(Int2ObjectMaps.singleton(fieldIndex, expr)));

        List<String> nodeNames = List.of("n1", "n2", "n3");
        Int2ObjectMap<NodeWithConsistencyToken> assignments = randomAssignments(table, nodeNames);
        ColocationGroup group = new ColocationGroup(LongList.of(0, 1, 2, 3), nodeNames, assignments);

        expectPartitionsPruned(table, columns, new Object[0], group, val);
    }

    @ParameterizedTest
    @MethodSource("nativeTypes")
    public void testDynamicParam(NativeType nativeType) {
        IgniteDistribution distribution = TestBuilders.affinity(List.of(0), 1, 1);

        IgniteTable table = TestBuilders.table()
                .name("T")
                .partitions(5)
                .addKeyColumn("c1", nativeType)
                .addColumn("c2", NativeTypes.INT32)
                .distribution(distribution)
                .build();

        int fieldIndex = 0;
        Object val = generateFieldValue(table, fieldIndex, nativeType);
        RexNode expr = newDynamicParam(table.descriptor(), 0);
        Object[] dynamicParameters = {val};

        PartitionPruningColumns columns = new PartitionPruningColumns(List.of(Int2ObjectMaps.singleton(fieldIndex, expr)));

        List<String> nodeNames = List.of("n1", "n2", "n3");
        Int2ObjectMap<NodeWithConsistencyToken> assignments = randomAssignments(table, nodeNames);
        ColocationGroup group = new ColocationGroup(LongList.of(0, 1, 2, 3), nodeNames, assignments);

        // TODO https://issues.apache.org/jira/browse/IGNITE-19162 Ignite doesn't support precision more than 3 for temporal types.
        if (nativeType instanceof TemporalNativeType) {
            TemporalNativeType temporalNativeType = (TemporalNativeType) nativeType;
            if (temporalNativeType.precision() > 3) {
                expectPartitionsNotPruned(table, columns, dynamicParameters, group);
                return;
            }
        }

        expectPartitionsPruned(table, columns, dynamicParameters, group, val);
    }

    private static void expectPartitionsNotPruned(
            IgniteTable table,
            PartitionPruningColumns pruningColumns,
            Object[] dynamicParameters,
            ColocationGroup group
    ) {

        ColocationGroup newGroup = PartitionPruningPredicate.prunePartitions(SOURCE_ID, table, pruningColumns, dynamicParameters, group);
        assertSame(newGroup, group, "Partitions should not have been pruned");
    }

    private static void expectPartitionsPruned(
            IgniteTable table,
            PartitionPruningColumns pruningColumns,
            Object[] dynamicParameters,
            ColocationGroup group,
            Object... values
    ) {
        Int2ObjectMap<NodeWithConsistencyToken> assignments = group.assignments();

        // Compute expected partitions using table's PartitionCalculator.
        PartitionWithConsistencyToken expectedPartition = computeExpectedPartition(table, group.assignments(), values);

        // Apply partition pruning to obtain new colocation group.
        ColocationGroup newGroup = PartitionPruningPredicate.prunePartitions(SOURCE_ID, table, pruningColumns, dynamicParameters, group);

        assertNotNull(newGroup);
        assertEquals(LongList.of(SOURCE_ID), newGroup.sourceIds(), "sourceIds");

        String expectedNode = assignments.get(expectedPartition.partId()).name();

        // Build expected map node -> partitions
        Map<String, List<PartitionWithConsistencyToken>> expected = new HashMap<>();
        for (String nodeName : group.nodeNames()) {
            expected.put(nodeName, List.of());
        }
        expected.put(expectedNode, List.of(expectedPartition));

        // Build actual map node -> partitions
        Map<String, List<PartitionWithConsistencyToken>> actual = new HashMap<>();
        for (String nodeName : group.nodeNames()) {
            List<PartitionWithConsistencyToken> actualPartitions = newGroup.partitionsWithConsistencyTokens(nodeName);
            actual.put(nodeName, actualPartitions);
        }

        assertEquals(expected, actual, "partitions per node (static)");

        // ensure both implementations of prunePartitions produce the same result.

        Map<String, List<PartitionWithConsistencyToken>> dynamicActual = new HashMap<>();

        for (String nodeName : group.nodeNames()) {
            ExecutionContext<Object[]> ctx = TestBuilders.executionContext()
                    .queryId(randomUUID())
                    .localNode(new ClusterNodeImpl(randomUUID(), nodeName, new NetworkAddress("localhost", 123)))
                    .executor(Mockito.mock(QueryTaskExecutor.class))
                    .dynamicParameters(dynamicParameters)
                    .build();
            ExpressionFactory expressionFactory = ctx.expressionFactory();

            List<PartitionWithConsistencyToken> result = PartitionPruningPredicate.prunePartitions(
                    ctx, pruningColumns, table, expressionFactory, assignments, nodeName
            );
            dynamicActual.put(nodeName, result);
        }

        assertEquals(expected, dynamicActual, "partitions per node (dynamic)");
    }

    private static PartitionWithConsistencyToken computeExpectedPartition(
            IgniteTable table,
            Map<Integer, NodeWithConsistencyToken> assignments,
            Object[] values
    ) {
        PartitionCalculator calculator = table.partitionCalculator().get();

        for (Object value : values) {
            calculator.append(value);
        }

        int p = calculator.partition();

        NodeWithConsistencyToken expected = assignments.get(p);

        return new PartitionWithConsistencyToken(p, expected.enlistmentConsistencyToken());
    }

    private static Int2ObjectMap<NodeWithConsistencyToken> randomAssignments(IgniteTable table, List<String> nodeNames) {
        Int2ObjectMap<NodeWithConsistencyToken> assignments = new Int2ObjectOpenHashMap<>();

        for (int i = 0; i < table.partitions(); ++i) {
            String nodeName = nodeNames.get(i % nodeNames.size());
            assignments.put(i, new NodeWithConsistencyToken(nodeName, i));
        }

        return assignments;
    }

    private Object generateFieldValue(IgniteTable table, int index, NativeType nativeType) {
        NativeType type = table.descriptor().columnDescriptor(index).physicalType();

        int precision;
        int scale;

        if (nativeType instanceof VarlenNativeType) {
            precision = ((VarlenNativeType) nativeType).length();
            scale = precision;
        } else if (nativeType instanceof TemporalNativeType) {
            precision = ((TemporalNativeType) nativeType).precision();
            scale = precision;
        } else if (nativeType instanceof DecimalNativeType) {
            precision = ((DecimalNativeType) nativeType).precision();
            scale = ((DecimalNativeType) nativeType).scale();
        } else {
            precision = 0;
            scale = precision;
        }

        Object val = SqlTestUtils.generateValueByType(type.spec(), precision, scale);
        assert val != null;

        log.info("Generated value for type {}: {}", nativeType, val);

        return val;
    }

    private static RexNode newDynamicParam(TableDescriptor descriptor, int paramIndex) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType rowType = descriptor.rowType(typeFactory, null);
        RelDataType fieldType = rowType.getFieldList().get(paramIndex).getType();
        RexBuilder rexBuilder = Commons.rexBuilder();

        return rexBuilder.makeDynamicParam(fieldType, paramIndex);
    }
}
