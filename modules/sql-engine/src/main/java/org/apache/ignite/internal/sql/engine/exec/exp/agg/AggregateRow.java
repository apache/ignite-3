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
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * Per group/grouping set row that contains state of accumulators.
 */
public final class AggregateRow<RowT> {

    public static volatile boolean ENABLED = true;

    public static final byte NO_GROUP_ID = -1;

    public final List<AccumulatorWrapper<RowT>> accs;

    private final AggregateType type;

    private final AccumulatorsStateImpl state;

    /** Constructor. */
    public AggregateRow(List<AccumulatorWrapper<RowT>> accs, IgniteTypeFactory typeFactory,
            AggregateType type, ImmutableBitSet groupFields, ImmutableBitSet allFields) {

        int rowSize = 0;

        for (AccumulatorWrapper<RowT> acc : accs) {
            rowSize += acc.stateTypes(typeFactory).size();
        }

        this.type = type;
        this.accs = accs;

        state = new AccumulatorsStateImpl(rowSize);
    }

    public static boolean addEmptyGroup(ImmutableBitSet groupKeys, AggregateType type) {
        return groupKeys.isEmpty() && (type == AggregateType.REDUCE || type == AggregateType.SINGLE);
    }

    /** Checks whether the given row matches a grouping set wtih the given id. */
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
        state.reset();

        if (type == AggregateType.MAP || type == AggregateType.SINGLE) {
            for (AccumulatorWrapper<RowT> acc : accs) {
                state.next(acc);

                acc.update(state, row);
            }
        } else {
            for (int i = 0; i < state.data.length; i++) {
                Object val = handler.get(i + allFields.cardinality(), row);
                state.initState(i, val);
            }

            for (AccumulatorWrapper<RowT> acc : accs) {
                state.next(acc);

                acc.combine(state, row);
            }
        }
    }

    /** Creates an empty array for fields to populate output row with. */
    public Object[] createOutput(ImmutableBitSet allFields, byte groupId) {
        int rowSize;

        if (type == AggregateType.MAP) {
            int extra = groupId == NO_GROUP_ID ? 0 : 1;

            rowSize = allFields.cardinality() + state.data.length + extra;
        } else {
            rowSize = allFields.cardinality() + accs.size();
        }

        return new Object[rowSize];
    }

    /** Writes collected state of the given aggregate row to given array. */
    public void writeTo(Object[] output, ImmutableBitSet allFields, byte groupId) {
        state.reset();

        if (type == AggregateType.MAP) {
            for (AccumulatorWrapper<RowT> acc : accs) {
                state.next(acc);

                acc.writeTo(state);
            }

            for (int i = 0; i < state.data.length; i++) {
                int pos = i + allFields.cardinality();
                output[pos] = state.data[i];
            }

            if (groupId != NO_GROUP_ID) {
                output[output.length - 1] = groupId;
            }
        } else {
            for (int i = 0; i < accs.size(); i++) {
                AccumulatorWrapper<RowT> wrapper = accs.get(i);
                Object val = wrapper.end();

                output[i + allFields.cardinality()] = val;
            }
        }
    }

    /** Utility method. */
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

    /** Utility method. */
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

    /** Utility method. */
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
            List<RelDataType> state = accumulators.getState(call);

            for (int j = 0; j < state.size(); j++) {
                String fieldName = format("_ACC_{}_FLD_{}", i, j);
                builder.add(fieldName, state.get(j));
            }
        }
    }

    private static class AccumulatorsStateImpl implements AccumulatorsState {

        private final Object[] data;

        private int position;

        private int maxPosition;

        AccumulatorsStateImpl(int rowSize) {
            this.data = new Object[rowSize];
        }

        void initState(int fieldIdx, @Nullable Object val) {
            data[fieldIdx] = val;
        }

        void next(AccumulatorWrapper<?> accumulator) {
            List<RelDataType> args = accumulator.stateTypes(Commons.typeFactory());

            position = maxPosition;
            maxPosition += args.size();
        }

        void reset() {
            position = 0;
            maxPosition = 0;
        }

        @Override
        public void set(int fieldIdx, @Nullable Object value) {
            int p = fieldIdx + position;

            assert p >= 0 && p < maxPosition :
                    format("Invalid position: {}. Index: {}. Min: {}, max: {}", p, fieldIdx, position, maxPosition);

            data[p] = value;
        }

        @Override
        public Object get(int fieldIdx) {
            return data[fieldIdx + position];
        }
    }
}
