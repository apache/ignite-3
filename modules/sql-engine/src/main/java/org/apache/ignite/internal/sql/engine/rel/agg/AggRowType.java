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


package org.apache.ignite.internal.sql.engine.rel.agg;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.BitSet;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

/**
 * Utility class to create row types for 2-phase aggregates.
 */
public final class AggRowType {

    public static final boolean ENABLED = true;

    private final RelDataType aggRowType;

    private AggRowType(RelDataType aggRowType) {
        this.aggRowType = aggRowType;
    }

    public RelDataType getAggRowType() {
        return aggRowType;
    }

    /** Utility method. */
    public static AggRowType sortAggRow(ImmutableBitSet grpKeys,
            IgniteTypeFactory typeFactory, RelDataType inputType, List<AggregateCall> aggregateCalls) {

        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (int fieldIdx : grpKeys) {
            RelDataTypeField fld = inputType.getFieldList().get(fieldIdx);
            builder.add(fld);
        }

        Accumulators accumulators = new Accumulators(typeFactory);

        for (int i = 0; i < aggregateCalls.size(); i++) {
            AggregateCall call = aggregateCalls.get(i);
            List<RelDataType> state = accumulators.getState(call);

            for (int j = 0; j < state.size(); j++) {
                String fieldName = format("_ACC_{}_FLD_{}", i, j);
                builder.add(fieldName, state.get(j));
            }
        }

        return new AggRowType(builder.build());
    }

    /** Utility method. */
    public static AggRowType hashAggrRow(List<ImmutableBitSet> groupSets,
            IgniteTypeFactory typeFactory, RelDataType inputType, List<AggregateCall> aggregateCalls) {

        Mapping mapping = computeFieldMapping(groupSets);

        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("_GROUP_ID", SqlTypeName.TINYINT);

        for (int i = 0; i < mapping.getTargetCount(); i++) {
            int source = mapping.getSource(i);
            RelDataTypeField fld = inputType.getFieldList().get(source);
            builder.add(fld);
        }

        Accumulators accumulators = new Accumulators(typeFactory);

        for (int i = 0; i < aggregateCalls.size(); i++) {
            AggregateCall call = aggregateCalls.get(i);
            List<RelDataType> state = accumulators.getState(call);

            for (int j = 0; j < state.size(); j++) {
                String fieldName = format("_ACC_{}_FLD_{}", i, j);
                builder.add(fieldName, state.get(j));
            }
        }

        return new AggRowType(builder.build());
    }

    /** Utility method. */
    public static Mapping computeFieldMapping(List<ImmutableBitSet> groupingSets) {
        BitSet fieldIndices = new BitSet();

        for (ImmutableBitSet groupingSet : groupingSets) {
            for (int field : groupingSet) {
                fieldIndices.set(field);
            }
        }

        Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldIndices.length(), fieldIndices.cardinality());
        int[] position = new int[1];

        fieldIndices.stream().forEach(b -> {
            int i = position[0];
            mapping.set(b, i);
            position[0] = i + 1;
        });

        return mapping;
    }
}
