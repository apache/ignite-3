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
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
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
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.jetbrains.annotations.Nullable;

/**
 * Partition predicate encapsulates partition pruning logic.
 */
public final class PartitionPruningPredicate {

    private static final ZoneId ZONE_ID_UTC = ZoneId.of("UTC");

    private final int tablePartitions;

    // TODO: https://issues.apache.org/jira/browse/IGNITE-21543 Remove after is resolved,
    //  remaining partitions should always be not null.
    @Nullable
    private final IntSet remainingPartitions;

    /**
     * Constructor.
     *
     * @param table Table.
     * @param pruningColumns Columns.
     * @param dynamicParameters Values of dynamic parameters.
     */
    public PartitionPruningPredicate(IgniteTable table, PartitionPruningColumns pruningColumns, Object[] dynamicParameters) {
        this.tablePartitions = table.partitions();
        this.remainingPartitions = computeRemainingPartitions(table, pruningColumns, dynamicParameters);
    }

    /**
     * Applies partition pruning to the given colocation group. This group should have the same number of assignments as the source table.
     *
     * @param colocationGroup Colocation group.
     *
     * @return New colocation group.
     */
    public ColocationGroup prunePartitions(ColocationGroup colocationGroup) {
        assert tablePartitions == colocationGroup.assignments().size() : "Number of partitions does not match";

        if (remainingPartitions == null) {
            return colocationGroup;
        }

        Map<String, List<PartitionWithConsistencyToken>> partitionsPerNode = newHashMap(colocationGroup.nodeNames().size());
        Set<String> newNodes = new HashSet<>();

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

        // Keep assignments intact, because they are used by DestinationFactory.
        return new ColocationGroup(
                colocationGroup.sourceIds(),
                List.copyOf(newNodes),
                colocationGroup.assignments(),
                partitionsPerNode
        );
    }

    @Nullable
    private static IntSet computeRemainingPartitions(
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

                // TODO: https://issues.apache.org/jira/browse/IGNITE-21543
                //  Remove after this issue makes it possible to have CAST('uuid_str' AS UUID) as value.
                if (physicalType.spec() == NativeTypeSpec.UUID) {
                    return null;
                }

                Object val = getNodeValue(physicalType, node, dynamicParameters);

                partitionCalculator.append(val);
            }

            int p = partitionCalculator.partition();
            remainingPartitions.add(p);
        }

        return remainingPartitions;
    }

    @Nullable
    private static Object getNodeValue(NativeType physicalType, RexNode node, Object[] dynamicParameters) {
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

    @Nullable
    private static Object getValueFromLiteral(NativeType physicalType, RexLiteral lit) {
        if (physicalType.spec() == NativeTypeSpec.DATE) {
            Calendar calendar = lit.getValueAs(Calendar.class);
            Instant instant = calendar.toInstant();
            return LocalDate.ofInstant(instant, ZONE_ID_UTC);
        } else if (physicalType.spec() == NativeTypeSpec.TIME) {
            Calendar calendar = lit.getValueAs(Calendar.class);
            Instant instant = calendar.toInstant();

            return LocalTime.ofInstant(instant, ZONE_ID_UTC);
        } else if (physicalType.spec() == NativeTypeSpec.TIMESTAMP) {
            TimestampString timestampString = lit.getValueAs(TimestampString.class);
            assert timestampString != null;

            return Instant.ofEpochMilli(timestampString.getMillisSinceEpoch());
        } else if (physicalType.spec() == NativeTypeSpec.DATETIME) {
            TimestampString timestampString = lit.getValueAs(TimestampString.class);
            assert timestampString != null;

            Instant instant = Instant.ofEpochMilli(timestampString.getMillisSinceEpoch());
            return LocalDateTime.ofInstant(instant, ZONE_ID_UTC);
        } else {
            Class<?> javaClass = NativeTypeSpec.toClass(physicalType.spec(), true);
            return lit.getValueAs(javaClass);
        }
    }
}
