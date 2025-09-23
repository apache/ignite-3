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

import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongList;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Partition predicate encapsulates partition pruning logic.
 */
public final class PartitionPruningPredicate {

    private static final ZoneId ZONE_ID_UTC = ZoneId.of("UTC");

    private PartitionPruningPredicate() {
    }

    /**
     * Applies partition pruning to the given colocation group. This group should have the same number of assignments as the source table.
     *
     * @param sourceId Source id.
     * @param table Table.
     * @param pruningColumns Partition pruning metadata.
     * @param dynamicParameters Values dynamic parameters.
     * @param colocationGroup Colocation group.
     * @return New colocation group.
     */
    public static ColocationGroup prunePartitions(
            long sourceId,
            IgniteTable table,
            PartitionPruningColumns pruningColumns,
            Object[] dynamicParameters,
            ColocationGroup colocationGroup) {

        assert table.partitions() == colocationGroup.assignments().size() : "Number of partitions does not match";

        IntSet remainingPartitions = computeRemainingPartitions(table, pruningColumns, dynamicParameters);
        if (remainingPartitions == null) {
            return colocationGroup;
        }

        Map<String, List<PartitionWithConsistencyToken>> partitionsPerNode = newHashMap(colocationGroup.nodeNames().size());
        Set<String> newNodes = new HashSet<>();
        Int2ObjectMap<NodeWithConsistencyToken> newAssignments = new Int2ObjectOpenHashMap<>(remainingPartitions.size());

        for (int p = 0; p < colocationGroup.assignments().size(); p++) {
            NodeWithConsistencyToken nodeWithConsistencyToken = colocationGroup.assignments().get(p);

            if (remainingPartitions.contains(p)) {
                newAssignments.put(p, nodeWithConsistencyToken);
            }
        }

        for (String nodeName : colocationGroup.nodeNames()) {
            List<PartitionWithConsistencyToken> partsWithConsistencyTokens = new ArrayList<>();

            for (int p = 0; p < colocationGroup.assignments().size(); p++) {
                NodeWithConsistencyToken nodeWithConsistencyToken = colocationGroup.assignments().get(p);
                if (!remainingPartitions.contains(p)) {
                    continue;
                }

                if (Objects.equals(nodeName, nodeWithConsistencyToken.name())) {
                    long t = nodeWithConsistencyToken.enlistmentConsistencyToken();

                    partsWithConsistencyTokens.add(new PartitionWithConsistencyToken(p, t));
                    newNodes.add(nodeName);
                }
            }

            if (!partsWithConsistencyTokens.isEmpty()) {
                partitionsPerNode.put(nodeName, partsWithConsistencyTokens);
            }
        }

        // Replace group id.
        return new ColocationGroup(
                LongList.of(sourceId),
                List.copyOf(newNodes),
                newAssignments,
                partitionsPerNode
        );
    }

    /**
     * Applies partition pruning to the list of given assignments and returns a list of partitions belonging to the given node.
     *
     * @param context Execution context.
     * @param pruningColumns Partition pruning metadata.
     * @param table Table.
     * @param expressionFactory Expression factory.
     * @param assignments Assignments.
     * @param nodeName Node name.
     * @return List of partitions that belong to the provided node.
     */
    public static <RowT> List<PartitionWithConsistencyToken> prunePartitions(
            ExecutionContext<RowT> context,
            PartitionPruningColumns pruningColumns,
            IgniteTable table,
            ExpressionFactory<RowT> expressionFactory,
            Int2ObjectMap<NodeWithConsistencyToken> assignments,
            String nodeName
    ) {
        ImmutableIntList keys = table.distribution().getKeys();
        PartitionCalculator partitionCalculator = table.partitionCalculator().get();
        List<PartitionWithConsistencyToken> result = new ArrayList<>();

        for (Int2ObjectMap<RexNode> columns : pruningColumns.columns()) {
            for (int key : keys) {
                RexNode node = columns.get(key);
                ColumnDescriptor descriptor = table.descriptor().columnDescriptor(key);
                NativeType physicalType = descriptor.physicalType();

                Object valueInInternalForm = expressionFactory.scalar(node).get(context);
                Object value = valueInInternalForm == null ? null : TypeUtils.fromInternal(valueInInternalForm, physicalType.spec());

                partitionCalculator.append(value);
            }

            int p = partitionCalculator.partition();
            NodeWithConsistencyToken token = assignments.get(p);

            if (nodeName.equals(token.name())) {
                result.add(new PartitionWithConsistencyToken(p, token.enlistmentConsistencyToken()));
            }
        }

        return result;
    }

    private static @Nullable IntSet computeRemainingPartitions(
            IgniteTable table,
            PartitionPruningColumns pruningColumns,
            Object[] dynamicParameters
    ) {
        ImmutableIntList keys = table.distribution().getKeys();
        IntSet remainingPartitions = new IntArraySet(pruningColumns.columns().size());
        PartitionCalculator partitionCalculator = table.partitionCalculator().get();

        for (Int2ObjectMap<RexNode> columns : pruningColumns.columns()) {
            for (int key : keys) {
                RexNode node = columns.get(key);
                NativeType physicalType = table.descriptor().columnDescriptor(key).physicalType();
                ColumnType columnType = physicalType.spec();
                Object val = getNodeValue(physicalType, node, dynamicParameters);

                // TODO https://issues.apache.org/jira/browse/IGNITE-19162 Ignite doesn't support precision more than 3 for temporal types.
                if (columnType == ColumnType.TIME
                        || columnType == ColumnType.DATETIME
                        || columnType == ColumnType.TIMESTAMP) {
                    TemporalNativeType temporalNativeType = (TemporalNativeType) physicalType;
                    if (temporalNativeType.precision() > 3) {
                        return null;
                    }
                }

                partitionCalculator.append(val);
            }

            int p = partitionCalculator.partition();
            remainingPartitions.add(p);
        }

        return remainingPartitions;
    }

    private static @Nullable Object getNodeValue(NativeType physicalType, RexNode node, Object[] dynamicParameters) {
        Object val;

        if (node instanceof RexLiteral) {
            val = getValueFromLiteral(physicalType, (RexLiteral) node);
        } else if (node instanceof RexDynamicParam) {
            RexDynamicParam dynamicParam = (RexDynamicParam) node;
            val = dynamicParameters[dynamicParam.getIndex()];
        } else {
            throw new IllegalArgumentException("Unexpected column value node: " + node);
        }

        return val;
    }

    private static @Nullable Object getValueFromLiteral(NativeType physicalType, RexLiteral lit) {
        if (physicalType.spec() == ColumnType.DATE) {
            Calendar calendar = lit.getValueAs(Calendar.class);
            Instant instant = calendar.toInstant();
            return LocalDate.ofInstant(instant, ZONE_ID_UTC);
        } else if (physicalType.spec() == ColumnType.TIME) {
            Calendar calendar = lit.getValueAs(Calendar.class);
            Instant instant = calendar.toInstant();

            return LocalTime.ofInstant(instant, ZONE_ID_UTC);
        } else if (physicalType.spec() == ColumnType.TIMESTAMP) {
            TimestampString timestampString = lit.getValueAs(TimestampString.class);
            assert timestampString != null;

            return Instant.ofEpochMilli(timestampString.getMillisSinceEpoch());
        } else if (physicalType.spec() == ColumnType.DATETIME) {
            TimestampString timestampString = lit.getValueAs(TimestampString.class);
            assert timestampString != null;

            Instant instant = Instant.ofEpochMilli(timestampString.getMillisSinceEpoch());
            return LocalDateTime.ofInstant(instant, ZONE_ID_UTC);
        } else {
            Class<?> javaClass = physicalType.spec().javaClass();
            return lit.getValueAs(javaClass);
        }
    }
}
