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

import java.util.List;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;

/**
 * Per group/grouping set row that contains state of accumulators.
 */
public final class AggregateRow<RowT> {
    /** A placeholder of absent group id. */
    public static final byte NO_GROUP_ID = -1;

    private final List<AccumulatorWrapper<RowT>> accs;

    private final AggregateType type;

    /** Constructor. */
    public AggregateRow(List<AccumulatorWrapper<RowT>> accs, AggregateType type) {
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
        int cardinality = allFields.cardinality();
        for (int i = 0; i < accs.size(); i++) {
            AccumulatorWrapper<RowT> wrapper = accs.get(i);
            output[i + cardinality] = wrapper.end();
        }

        if (groupId != NO_GROUP_ID && type == AggregateType.MAP) {
            output[output.length - 1] = groupId;
        }
    }

}
