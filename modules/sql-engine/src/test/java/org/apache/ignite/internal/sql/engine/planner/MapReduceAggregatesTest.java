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

package org.apache.ignite.internal.sql.engine.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.AggregateRelBuilder;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MapReduceAggregates}.
 */
public class MapReduceAggregatesTest {

    /**
     * Checks that mapping is applied to {@code groupSet} and {@code groupSets} arguments to methods of {@link AggregateRelBuilder}.
     */
    @Test
    public void testGroupSetMapping() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        RelDataType rowType = typeFactory.builder().add("f1", SqlTypeName.INTEGER)
                .add("f2", SqlTypeName.INTEGER)
                .build();

        RelOptCluster cluster = Commons.emptyCluster();
        RelTraitSet traitSet = RelTraitSet.createEmpty();

        // Input to aggregate
        LogicalValues values = new LogicalValues(cluster, traitSet, rowType, ImmutableList.of());

        // Aggregate
        ImmutableBitSet groupSet = ImmutableBitSet.of(1);
        AggregateCall aggregateCall = newCall(typeFactory, List.of(1));

        LogicalAggregate aggregate = new LogicalAggregate(cluster, traitSet, List.of(), values,
                groupSet, List.of(groupSet), List.of(aggregateCall));

        Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, 2, 5);
        mapping.set(1, 3);

        GroupSetCollector collect = new GroupSetCollector(rowType);

        MapReduceAggregates.buildAggregates(aggregate, collect, mapping);

        Pair<ImmutableBitSet, List<ImmutableBitSet>> mapGroups = collect.collectedGroupSets.get(0);
        Pair<ImmutableBitSet, List<ImmutableBitSet>> reduceGroups = collect.collectedGroupSets.get(1);
        ImmutableBitSet mappedGroupSet = Mappings.apply(mapping, groupSet);

        assertEquals(Pair.of(groupSet, List.of(groupSet)), mapGroups, "group sets on MAP phase");
        assertEquals(Pair.of(mappedGroupSet, List.of(mappedGroupSet)), reduceGroups, "group sets on REDUCE phase");
    }

    private static AggregateCall newCall(IgniteTypeFactory typeFactory, List<Integer> args) {
        return AggregateCall.create(SqlStdOperatorTable.COUNT,
                false, false, false, ImmutableList.of(), args, -1, null,
                RelCollations.EMPTY,
                typeFactory.createSqlType(SqlTypeName.BIGINT),
                "count");
    }

    private static class GroupSetCollector implements AggregateRelBuilder {
        private final List<Pair<ImmutableBitSet, List<ImmutableBitSet>>> collectedGroupSets = new ArrayList<>();
        private final RelDataType rowType;

        GroupSetCollector(RelDataType rowType) {
            this.rowType = rowType;
        }

        @Override
        public IgniteRel makeMapAgg(RelOptCluster cluster,
                RelNode input,
                ImmutableBitSet groupSet,
                List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggregateCalls) {

            collectedGroupSets.add(Pair.of(groupSet, groupSets));

            return createOutExpr(cluster, input);
        }

        @Override
        public IgniteRel makeProject(RelOptCluster cluster, RelNode input, List<RexNode> reduceInputExprs, RelDataType projectRowType) {
            return new IgniteProject(cluster, input.getTraitSet(), input, reduceInputExprs, projectRowType);
        }

        @Override
        public IgniteRel makeReduceAgg(RelOptCluster cluster,
                RelNode input,
                ImmutableBitSet groupSet,
                List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggregateCalls,
                RelDataType outputType) {

            collectedGroupSets.add(Pair.of(groupSet, groupSets));

            return createOutExpr(cluster, input);
        }

        private IgniteValues createOutExpr(RelOptCluster cluster, RelNode input) {
            // Expression can be of any type and implementation of MapReduceAggregates should not probe into it.
            return new IgniteValues(cluster, rowType, ImmutableList.of(), input.getTraitSet());
        }
    }
}
