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

import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * IgniteReduceAggregateBase.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class IgniteReduceAggregateBase extends SingleRel implements TraitsAwareIgniteRel {
    protected final ImmutableBitSet groupSet;

    protected final List<ImmutableBitSet> groupSets;

    protected final List<AggregateCall> aggCalls;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    protected IgniteReduceAggregateBase(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RelDataType rowType
    ) {
        super(cluster, traits, input);

        assert rowType != null;
        this.groupSet = groupSet;
        if (groupSets == null) {
            groupSets = List.of(groupSet);
        }
        this.groupSets = groupSets;
        this.aggCalls = aggCalls;
        this.rowType = rowType;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    protected IgniteReduceAggregateBase(RelInput input) {
        this(
                input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getInput(),
                input.getBitSet("group"),
                input.getBitSetList("groups"),
                input.getAggregateCalls("aggs"),
                input.getRowType("rowType"));
    }

    /** {@inheritDoc} */
    @Override
    protected RelDataType deriveRowType() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw)
                .itemIf("rowType", rowType, pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES)
                .item("group", groupSet)
                .itemIf("groups", groupSets, Group.induce(groupSet, groupSets) != Group.SIMPLE)
                .itemIf("aggs", aggCalls, pw.nest());

        if (!pw.nest()) {
            for (Ord<AggregateCall> ord : Ord.zip(aggCalls)) {
                pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
            }
        }

        return pw;
    }

    /**
     * Get group set.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ImmutableBitSet getGroupSet() {
        return groupSet;
    }

    /**
     * Set group sets.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public List<ImmutableBitSet> getGroupSets() {
        return groupSets;
    }

    /**
     * Get aggregate calls.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public List<AggregateCall> getAggregateCalls() {
        return aggCalls;
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        if (TraitUtils.distribution(nodeTraits) == IgniteDistributions.single()) {
            return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(IgniteDistributions.single())));
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        RelTraitSet in = inputTraits.get(0);

        return List.of(
                Pair.of(
                        nodeTraits.replace(IgniteDistributions.single()),
                        List.of(in.replace(IgniteDistributions.single()))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        // Reduce aggregate doesn't change result's row count until we don't use
        // cluster parallelism at the model (devide source rows by nodes for partitioned data).
        return mq.getRowCount(getInput());
    }
}
