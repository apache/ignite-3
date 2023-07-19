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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.BitSet;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

/**
 * Per group/grouping set row that contains state of accumulators.
 */
public final class AggregateRow<RowT> {

    public static volatile boolean ENABLED = true;

    public static final byte NO_GROUP_ID = -1;

    public final List<AccumulatorWrapper<RowT>> accs;

    private final AggregateType type;

    /** Constructor. */
    public AggregateRow(List<AccumulatorWrapper<RowT>> accs, IgniteTypeFactory typeFactory,
            AggregateType type, ImmutableBitSet groupFields, ImmutableBitSet allFields) {

        this.type = type;
        this.accs = accs;
    }

    /** Initialized an empty group if necessary. */
    public static boolean addEmptyGroup(ImmutableBitSet groupKeys, AggregateType type) {
        return groupKeys.isEmpty() && (type == AggregateType.REDUCE || type == AggregateType.SINGLE);
    }

    /** Checks whether the given row matches a grouping set with the given id. */
    public static <RowT> boolean groupMatches(RowHandler<RowT> handler, RowT row, AggregateType type, byte groupId) {
        if (type == AggregateType.REDUCE) {
            int columnCount = handler.columnCount(row);
            byte targetGroupId = (byte) handler.get(columnCount - 1, row);

            return targetGroupId == groupId;
        } else {
            return groupId != NO_GROUP_ID;
        }
    }

    /** Updates this row by using data of the given row. */
    public void update(ImmutableBitSet allFields, RowHandler<RowT> handler, RowT row) {
        for (AccumulatorWrapper<RowT> acc : accs) {
            acc.add(row);
        }
    }

    /** Creates an empty array for fields to populate output row with. */
    public Object[] createOutput(ImmutableBitSet allFields, byte groupId) {
        int extra = groupId == NO_GROUP_ID || type != AggregateType.MAP ? 0 : 1;
        int rowSize = allFields.cardinality() + accs.size() + extra;

        return new Object[rowSize];
    }

    /** Writes aggregate state of the given row to given array. */
    public void writeTo(Object[] output, ImmutableBitSet allFields, byte groupId) {
        for (int i = 0; i < accs.size(); i++) {
            AccumulatorWrapper<RowT> wrapper = accs.get(i);
            output[i + allFields.cardinality()] = wrapper.end();
        }

        if (groupId != NO_GROUP_ID && type == AggregateType.MAP) {
            output[output.length - 1] = groupId;
        }
    }

    /**
     * Creates a row type for REDUCE phase of a two phase aggregate used by hash-based map/reduce implementation.
     *
     * <p>Given the grouping keys f0, f1, f2 and aggregates (agg1 and agg2) in produces the following type:
     *
     * <pre>
     *     field_0 -> f0_type
     *     field_1 -> f1_type
     *     field_2 -> f2_type
     *     field_3 -> agg1_result_type
     *     field_4 -> agg2_result_type
     * </pre>
     */
    public static RelDataType createSortRowType(ImmutableBitSet grpKeys,
            IgniteTypeFactory typeFactory, RelDataType inputType, List<AggregateCall> aggregateCalls) {

        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (int fieldIdx : grpKeys) {
            RelDataTypeField fld = inputType.getFieldList().get(fieldIdx);
            builder.add(fld);
        }

        addAccumulatorFields(typeFactory, aggregateCalls, builder);

        return builder.build();
    }

    /**
     * Creates a row type returned from a MAP phase of a two phase aggregate used by hash-based map/reduce implementation.
     *
     * <p>Given the input row [f0, f1, f2, f3, f4], grouping sets [f1], [f2, f0], [f4], and 2 aggregates (agg1 and agg2)
     * it produces the following type:
     *
     * <pre>
     *     field_0 -> f0_type
     *     field_1 -> f1_type
     *     field_2 -> f2_type
     *     field_3 -> f4_type
     *     field_4 -> agg1_result_type
     *     field_5 -> agg2_result_type
     *     _group_id -> byte # each grouping set has id assigned to id and that id is equal to its position in GROUP BY GROUPING SET clause.
     * </pre>
     */
    public static RelDataType createHashRowType(List<ImmutableBitSet> groupSets,
            IgniteTypeFactory typeFactory, RelDataType inputType, List<AggregateCall> aggregateCalls) {

        Mapping mapping = computeFieldMapping(groupSets, AggregateType.REDUCE);

        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (int i = 0; i < mapping.getTargetCount(); i++) {
            int source = mapping.getSource(i);
            RelDataTypeField fld = inputType.getFieldList().get(source);
            builder.add(fld);
        }

        addAccumulatorFields(typeFactory, aggregateCalls, builder);

        builder.add("_GROUP_ID", SqlTypeName.TINYINT);

        return builder.build();
    }

    /**
     * Field mapping for hash execution nodes. See {@link #createHashRowType(List, IgniteTypeFactory, RelDataType, List)}.
     *
     * <p>For REDUCE phase it produce the following mapping:
     *
     * <pre>
     * group sets:
     *   [0], [2, 0], [3]
     * mapping:
     *   0 -> 0
     *   2 -> 1
     *   3 -> 2
     * </pre>
     *
     * <p>Generates identity mapping for other phases.
     */
    public static Mapping computeFieldMapping(List<ImmutableBitSet> groupingSets, AggregateType aggregateType) {
        BitSet fieldIndices = new BitSet();

        for (ImmutableBitSet groupingSet : groupingSets) {
            for (int field : groupingSet) {
                fieldIndices.set(field);
            }
        }

        if (aggregateType == AggregateType.REDUCE) {
            Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldIndices.length(), fieldIndices.cardinality());
            int[] position = new int[1];

            fieldIndices.stream().forEach(b -> {
                int i = position[0];
                mapping.set(b, i);
                position[0] = i + 1;
            });

            return mapping;
        } else {
            return Mappings.createIdentity(fieldIndices.length());
        }

    }

    private static void addAccumulatorFields(IgniteTypeFactory typeFactory, List<AggregateCall> aggregateCalls, Builder builder) {
        Accumulators accumulators = new Accumulators(typeFactory);

        for (int i = 0; i < aggregateCalls.size(); i++) {
            AggregateCall call = aggregateCalls.get(i);

            Accumulator acc = accumulators.accumulatorFactory(call).get();
            RelDataType fieldType = acc.returnType(typeFactory);
            String fieldName = format("_ACC{}", i);

            builder.add(fieldName, fieldType);
        }
    }
}
