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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * IgniteSingleAggregateBase.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class IgniteColocatedAggregateBase extends IgniteAggregate implements TraitsAwareIgniteRel {
    /** {@inheritDoc} */
    IgniteColocatedAggregateBase(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    IgniteColocatedAggregateBase(RelInput input) {
        super(TraitUtils.changeTraits(input, IgniteConvention.INSTANCE));
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
        IgniteDistribution inDistribution = TraitUtils.distribution(inputTraits.get(0));

        // If there is only single stream of data, it's colocated for sure
        if (inDistribution.satisfies(IgniteDistributions.single())) {
            return List.of(Pair.of(nodeTraits.replace(inDistribution), inputTraits));
        }

        // for empty group the only option is to bring all data to the single place
        if (groupSet.isEmpty()) {
            return List.of(Pair.of(
                    nodeTraits.replace(IgniteDistributions.single()),
                    List.of(inputTraits.get(0).replace(IgniteDistributions.single()))
            ));
        }

        // Otherwise if stream is distributed by hash function, and grouping columns is a super-set of
        // distribution keys, we can use colocated aggregate as well
        if (inDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            if (groupSet.contains(ImmutableBitSet.of(inDistribution.getKeys()))) {
                // Group set contains all distribution keys, shift distribution keys according to used columns.
                IgniteDistribution outDistribution = inDistribution.apply(Commons.trimmingMapping(rowType.getFieldCount(), groupSet));

                return List.of(Pair.of(nodeTraits.replace(outDistribution), inputTraits));
            }
        }

        // Rows belonging to the same group are not colocated, thus let's create
        // distribution that will satisfy for sure. It's important to return here
        // at least one option, otherwise propagation result for other traits will be
        // dropped for current input.
        IgniteDistribution newInDistribution = IgniteDistributions.hash(groupSet.asList());
        IgniteDistribution newOutDistribution = newInDistribution.apply(Commons.trimmingMapping(rowType.getFieldCount(), groupSet));

        return List.of(
                Pair.of(
                        nodeTraits.replace(IgniteDistributions.single()),
                        List.of(inputTraits.get(0).replace(IgniteDistributions.single()))
                ),
                Pair.of(
                        nodeTraits.replace(newOutDistribution),
                        List.of(inputTraits.get(0).replace(newInDistribution))
                )
        );
    }
}
