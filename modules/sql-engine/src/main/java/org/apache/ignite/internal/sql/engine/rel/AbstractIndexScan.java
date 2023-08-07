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

package org.apache.ignite.internal.sql.engine.rel;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.externalize.RelInputEx;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * Class with index conditions info.
 */
public abstract class AbstractIndexScan extends ProjectableFilterableTableScan {
    protected final String idxName;

    protected final @Nullable List<SearchBounds> searchBounds;

    protected final Type type;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    AbstractIndexScan(RelInput input) {
        super(input);
        idxName = input.getString("index");
        type = input.getEnum("type", Type.class);
        searchBounds = ((RelInputEx) input).getSearchBounds("searchBounds");
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    protected AbstractIndexScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelOptTable table,
            String idxName,
            Type type,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable List<SearchBounds> searchBounds,
            @Nullable ImmutableBitSet reqColumns
    ) {
        super(cluster, traitSet, hints, table, proj, cond, reqColumns);

        this.idxName = idxName;
        this.type = type;
        this.searchBounds = searchBounds;
    }

    /** {@inheritDoc} */
    @Override
    protected RelWriter explainTerms0(RelWriter pw) {
        pw = pw.item("index", idxName)
                .item("type", type.name())
                .itemIf("searchBounds", searchBounds, searchBounds != null);
        return super.explainTerms0(pw);
    }

    /** {@inheritDoc} */
    @Override
    public RelNode accept(RexShuttle shuttle) {
        if (searchBounds != null) {
            List<RexNode> expressions = searchBounds.stream()
                    .filter(Objects::nonNull)
                    .map(SearchBounds::condition)
                    .collect(Collectors.toList());

            shuttle.apply(expressions);
        }

        return super.accept(shuttle);
    }

    /** Return index name. */
    public String indexName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = table.getRowCount();

        double cost = 0;

        if (type == Type.HASH) {
            boolean notExact = (searchBounds() == null)
                    || searchBounds().stream().anyMatch(bound -> bound.type() == SearchBounds.Type.RANGE);

            if (!notExact) {
                notExact = searchBounds().stream().flatMap(bound -> bound instanceof MultiBounds
                                ? ((MultiBounds) bound).bounds().stream() : Stream.of(bound))
                        .anyMatch(bound -> bound.type() != SearchBounds.Type.EXACT);
            }

            if (notExact) {
                // now bounds index scan is only available for sorted index, check:
                // PartitionReplicaListener#processReadOnlyScanRetrieveBatchAction
                return planner.getCostFactory().makeInfiniteCost();
            }
        }

        if (condition == null) {
            cost = rows * IgniteCost.ROW_PASS_THROUGH_COST;
        } else {
            double selectivity = 1;

            RexBuilder builder = getCluster().getRexBuilder();

            if (searchBounds() != null) {
                selectivity = mq.getSelectivity(this, RexUtil.composeConjunction(builder,
                        Commons.transform(searchBounds(), b -> b == null ? null : b.condition())));

                cost = Math.log(rows) * IgniteCost.ROW_COMPARISON_COST;
            }

            rows *= selectivity;

            if (rows <= 0) {
                rows = 1;
            }

            cost += rows * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST);
        }

        // additional tiny cost for preventing equality with table scan.
        return planner.getCostFactory().makeCost(rows, cost, 0).plus(planner.getCostFactory().makeTinyCost());
    }

    /**
     * Get index conditions.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public List<SearchBounds> searchBounds() {
        return searchBounds;
    }
}
