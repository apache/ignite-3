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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

public final class AggRowType {

    public static final boolean ENABLED = true;

    private final RelDataType aggRowType;

    private AggRowType(RelDataType aggRowType) {
        this.aggRowType = aggRowType;
    }

    public RelDataType getAggRowType() {
        return aggRowType;
    }

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

            for (int j = 0; j < state.size(); j ++) {
                builder.add("ACC_" + i + "_FLD_" + j, state.get(j));
            }
        }

        return new AggRowType(builder.build());
    }

    public static List<AggRowType> hashAggrRows(List<ImmutableBitSet> groupSets,
            IgniteTypeFactory typeFactory, RelDataType inputType, List<AggregateCall> aggregateCalls) {

        List<AggRowType> rowTypes = new ArrayList<>(groupSets.size());

        for (ImmutableBitSet group : groupSets) {
            AggRowType aggRowType = sortAggRow(group, typeFactory, inputType, aggregateCalls);

            rowTypes.add(aggRowType);
        }

        return rowTypes;
    }
}
