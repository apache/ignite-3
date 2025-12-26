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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.SINGLE;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlComparator;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.MapReduceAgg;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.StructNativeType;

/**
 * SortAggregateExecutionTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SortAggregateExecutionTest extends BaseAggregateTest {
    /** {@inheritDoc} */
    @Override
    protected SingleNode<Object[]> createColocatedAggregateNodesChain(
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
            AggregateCall call,
            RelDataType inRowType,
            ScanNode<Object[]> scan,
            boolean group
    ) {
        assert grpSets.size() == 1;

        ImmutableBitSet grpSet = first(grpSets);

        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(grpSet.asList()));

        Comparator<Object[]> cmp;
        if (grpSet.isEmpty() && (collation == null || nullOrEmpty(collation.getFieldCollations()))) {
            cmp = (k1, k2) -> 0;
        } else {
            SqlComparator comparator = ctx.expressionFactory().comparator(collation);

            cmp = (r1, r2) -> comparator.compare(ctx, r1, r2);
        }

        StructNativeType outputRowSchema = createOutputSchema(ctx, call, inRowType, grpSet);
        RowFactory<Object[]> outputRowFactory = ctx.rowFactoryFactory().create(outputRowSchema);

        SortAggregateNode<Object[]> agg = new SortAggregateNode<>(
                ctx,
                SINGLE,
                grpSet,
                accFactory(ctx, call, SINGLE, inRowType),
                outputRowFactory,
                cmp
        );

        if (group) {
            SortNode<Object[]> sort = new SortNode<>(ctx, cmp);

            sort.register(scan);

            agg.register(sort);
        } else {
            agg.register(scan);
        }

        return agg;
    }

    /** {@inheritDoc} */
    @Override
    protected SingleNode<Object[]> createMapReduceAggregateNodesChain(
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
            AggregateCall call,
            RelDataType inRowType,
            ScanNode<Object[]> scan,
            boolean group
    ) {
        assert grpSets.size() == 1;

        ImmutableBitSet grpSet = first(grpSets);

        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(grpSet.asList()));

        Comparator<Object[]> cmp;
        if (grpSet.isEmpty() && (collation == null || nullOrEmpty(collation.getFieldCollations()))) {
            cmp = (k1, k2) -> 0;
        } else {
            SqlComparator comparator = ctx.expressionFactory().comparator(collation);

            cmp = (r1, r2) -> comparator.compare(ctx, r1, r2);
        }

        // Map node
        RowFactory<Object[]> mapRowFactory = ctx.rowFactoryFactory().create(TypeUtils.convertStructuredType(inRowType));

        SortAggregateNode<Object[]> aggMap = new SortAggregateNode<>(
                ctx,
                MAP,
                grpSet,
                accFactory(ctx, call, MAP, inRowType),
                mapRowFactory,
                cmp
        );

        if (group) {
            SortNode<Object[]> sort = new SortNode<>(ctx, cmp);

            sort.register(scan);

            aggMap.register(sort);
        } else {
            aggMap.register(scan);
        }

        // Reduce node

        // The group's fields placed on the begin of the output row (planner
        // does this by Projection node for aggregate input).
        ImmutableIntList reduceGrpFields = ImmutableIntList.copyOf(
                IntStream.range(0, grpSet.cardinality()).boxed().collect(Collectors.toList())
        );

        RelCollation rdcCollation = RelCollations.of(reduceGrpFields);

        Comparator<Object[]> rdcCmp;
        if (grpSet.isEmpty() && (rdcCollation == null || nullOrEmpty(rdcCollation.getFieldCollations()))) {
            rdcCmp = (k1, k2) -> 0;
        } else {
            SqlComparator comparator = ctx.expressionFactory().comparator(rdcCollation);

            rdcCmp = (r1, r2) -> comparator.compare(ctx, r1, r2);
        }

        Mapping mapping = Commons.trimmingMapping(grpSet.length(), grpSet);
        MapReduceAgg mapReduceAgg = MapReduceAggregates.createMapReduceAggCall(
                Commons.cluster(),
                call,
                mapping.getTargetCount(),
                inRowType,
                true
        );

        StructNativeType outputRowSchema = createOutputSchema(ctx, call, inRowType, grpSet);
        RowFactory<Object[]> outputRowFactory = ctx.rowFactoryFactory().create(outputRowSchema);

        SortAggregateNode<Object[]> aggRdc = new SortAggregateNode<>(
                ctx,
                REDUCE,
                ImmutableBitSet.of(reduceGrpFields),
                accFactory(ctx, mapReduceAgg.getReduceCall(), REDUCE, inRowType),
                outputRowFactory,
                rdcCmp
        );

        aggRdc.register(aggMap);

        return aggRdc;
    }
}
