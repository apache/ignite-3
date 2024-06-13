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

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Tests for {@link PartitionPruningPredicate}.
 */
public class PartitionPruningPredicateSelfTest extends BaseIgniteAbstractTest {

    private final long seed = System.nanoTime();

    @BeforeEach
    public void beforeEach() {
        log.info("Seed is {}", seed);
    }

    private static List<ColumnType> columnTypes() {
        return Arrays.stream(ColumnType.values())
                .filter(t -> t != ColumnType.NULL
                        // TODO https://issues.apache.org/jira/browse/IGNITE-18431 BitSet is not supported.
                        && t != ColumnType.BITMASK
                        && t != ColumnType.NUMBER
                        // TODO https://issues.apache.org/jira/browse/IGNITE-15200 Include interval types after this issue is resolved
                        && t != ColumnType.DURATION
                        && t != ColumnType.PERIOD
                )
                .collect(Collectors.toList());
    }

    @ParameterizedTest
    @MethodSource("columnTypes")
    public void testLiteralValue(ColumnType columnType) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-21543 Remove after is resolved,
        //  because it allows to support CAST('uuid-str' AS UUID) expressions.
        Assumptions.assumeFalse(columnType == ColumnType.UUID);

        IgniteDistribution distribution = IgniteDistributions.affinity(List.of(0), 1, 1);

        NativeType nativeType = TypeUtils.columnType2NativeType(columnType, 3, 2, 3);

        IgniteTable table = TestBuilders.table()
                .name("T")
                .partitions(5)
                .addKeyColumn("c1", nativeType)
                .addColumn("c2", NativeTypes.INT32)
                .distribution(distribution)
                .build();

        int fieldIndex = 0;
        Object val = generateFieldValue(table, fieldIndex);
        RexNode expr = generateLiteralOrValueExpr(columnType, val);

        PartitionPruningColumns columns = new PartitionPruningColumns(List.of(Int2ObjectMaps.singleton(fieldIndex, expr)));

        List<String> nodeNames = List.of("n1", "n2", "n3");
        Int2ObjectMap<NodeWithConsistencyToken> assignments = randomAssignments(table, nodeNames);
        ColocationGroup group = new ColocationGroup(List.of(0L), nodeNames, assignments);

        expectPartitionsPruned(table, columns, new Object[0], group, val);
    }

    @ParameterizedTest
    @MethodSource("columnTypes")
    public void testDynamicParam(ColumnType columnType) {
        IgniteDistribution distribution = IgniteDistributions.affinity(List.of(0), 1, 1);

        NativeType nativeType = TypeUtils.columnType2NativeType(columnType, 3, 2, 3);

        IgniteTable table = TestBuilders.table()
                .name("T")
                .partitions(5)
                .addKeyColumn("c1", nativeType)
                .addColumn("c2", NativeTypes.INT32)
                .distribution(distribution)
                .build();

        int fieldIndex = 0;
        Object val = generateFieldValue(table, fieldIndex);
        RexNode expr = newDynamicParam(table.descriptor(), 0);
        Object[] dynamicParameters = {val};

        PartitionPruningColumns columns = new PartitionPruningColumns(List.of(Int2ObjectMaps.singleton(fieldIndex, expr)));

        List<String> nodeNames = List.of("n1", "n2", "n3");
        Int2ObjectMap<NodeWithConsistencyToken> assignments = randomAssignments(table, nodeNames);
        ColocationGroup group = new ColocationGroup(List.of(0L), nodeNames, assignments);

        expectPartitionsPruned(table, columns, dynamicParameters, group, val);
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
        ColocationGroup newGroup = PartitionPruningPredicate.prunePartitions(table, pruningColumns, dynamicParameters, group);

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
                    .queryId(UUID.randomUUID())
                    .localNode(new ClusterNodeImpl(nodeName, nodeName, new NetworkAddress("localhost", 123)))
                    .executor(Mockito.mock(QueryTaskExecutor.class))
                    .dynamicParameters(dynamicParameters)
                    .build();
            ExpressionFactory<Object[]> expressionFactory = ctx.expressionFactory();

            List<PartitionWithConsistencyToken> result = PartitionPruningPredicate.prunePartitions(
                    pruningColumns, table, expressionFactory, assignments, nodeName
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

    private Object generateFieldValue(IgniteTable table, int index) {
        ColumnType columnType = table.descriptor().columnDescriptor(index).physicalType().spec().asColumnType();

        Random current = new Random();
        current.setSeed(seed);

        Object val = SqlTestUtils.generateValueByType(current.nextInt(100), columnType);
        assert val != null;

        // Adjust Decimal for the necessary scale and precision.
        if (val instanceof BigDecimal) {
            val = BigDecimal.ONE;
        }

        return val;
    }

    private static RexNode generateLiteralOrValueExpr(ColumnType type, Object value) {
        RexBuilder rexBuilder = Commons.rexBuilder();
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        switch (type) {
            case NULL:
                return rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
            case BOOLEAN:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BOOLEAN));
            case INT8:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.TINYINT));
            case INT16:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.SMALLINT));
            case INT32:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.INTEGER));
            case INT64:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BIGINT));
            case FLOAT:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.REAL));
            case DOUBLE:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.DOUBLE));
            case DECIMAL:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.DECIMAL));
            case DATE:
                LocalDate localDate = (LocalDate) value;
                int epochDay = (int) localDate.toEpochDay();

                return rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(epochDay));
            case TIME:
                LocalTime time = (LocalTime) value;
                int millisOfDay = (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());

                return rexBuilder.makeTimeLiteral(TimeString.fromMillisOfDay(millisOfDay), 3);
            case DATETIME:
                LocalDateTime localDateTime = (LocalDateTime) value;
                Instant instant1 = localDateTime.toInstant(ZoneOffset.UTC);
                TimestampString timestampString = TimestampString.fromMillisSinceEpoch(instant1.toEpochMilli());

                return rexBuilder.makeTimestampWithLocalTimeZoneLiteral(timestampString, 3);
            case TIMESTAMP:
                Instant instant = (Instant) value;

                return rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(instant.toEpochMilli()), 3);
            case UUID:
                RexLiteral uuidStr = rexBuilder.makeLiteral(value.toString(), typeFactory.createSqlType(SqlTypeName.VARCHAR));
                IgniteCustomType uuidType = typeFactory.createCustomType(UuidType.NAME);

                return rexBuilder.makeCast(uuidType, uuidStr);
            case BITMASK:
                throw new IllegalArgumentException("Not supported: " + type);
            case STRING:
                return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.VARCHAR));
            case BYTE_ARRAY:
                byte[] bytes = (byte[]) value;
                ByteString byteStr = new ByteString(bytes);
                return rexBuilder.makeLiteral(byteStr, typeFactory.createSqlType(SqlTypeName.VARBINARY));
            case PERIOD:
            case DURATION:
            case NUMBER:
                throw new IllegalArgumentException("Not supported: " + type);
            default:
                throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static RexNode newDynamicParam(TableDescriptor descriptor, int paramIndex) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType rowType = descriptor.rowType(typeFactory, null);
        RelDataType fieldType = rowType.getFieldList().get(paramIndex).getType();
        RexBuilder rexBuilder = Commons.rexBuilder();

        return rexBuilder.makeDynamicParam(fieldType, paramIndex);
    }
}
