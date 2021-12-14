/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rel;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

/**
 * Relational expression that computes a set of 'select expressions' from its input relational expression.
 */
public class IgniteProject extends Project implements TraitsAwareIgniteRel {
    /**
     * Creates a Project.
     *
     * @param cluster  Cluster that this relational expression belongs to.
     * @param traits   Traits of this relational expression.
     * @param input    Input relational expression.
     * @param projects List of expressions for the input columns.
     * @param rowType  Output row type.
     */
    public IgniteProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, List.of(), input, projects, rowType);
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteProject(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new IgniteProject(getCluster(), traitSet, input, projects, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // All distribution types except hash distribution are propagated as is.
        // In case of hash distribution we need to project distribution keys.
        // In case one of distribution keys is erased by projection result distribution
        // becomes default single since we cannot calculate required input distribution.

        RelTraitSet in = inputTraits.get(0);
        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        if (distribution.getType() != HASH_DISTRIBUTED) {
            return Pair.of(nodeTraits, List.of(in.replace(distribution)));
        }

        Mappings.TargetMapping mapping = getPartialMapping(
                input.getRowType().getFieldCount(), getProjects());

        ImmutableIntList keys = distribution.getKeys();
        IntList srcKeys = new IntArrayList(keys.size());

        for (int key : keys) {
            int src = mapping.getSourceOpt(key);

            if (src == -1) {
                break;
            }

            srcKeys.add(src);
        }

        if (srcKeys.size() == keys.size()) {
            return Pair.of(nodeTraits, List.of(in.replace(hash(ImmutableIntList.copyOf(srcKeys), distribution.function()))));
        }

        return Pair.of(nodeTraits.replace(single()), List.of(in.replace(single())));
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The code below projects required collation. In case we cannot calculate required source collation
        // (e.g. one of required sorted fields is result of a function call), input and output collations are erased.

        RelTraitSet in = inputTraits.get(0);

        List<RelFieldCollation> fieldCollations = TraitUtils.collation(nodeTraits).getFieldCollations();

        if (fieldCollations.isEmpty()) {
            return Pair.of(nodeTraits, List.of(in.replace(RelCollations.EMPTY)));
        }

        Int2IntOpenHashMap targets = new Int2IntOpenHashMap();
        for (Ord<RexNode> project : Ord.zip(getProjects())) {
            if (project.e instanceof RexInputRef) {
                targets.putIfAbsent(project.i, ((RexSlot) project.e).getIndex());
            }
        }

        List<RelFieldCollation> inFieldCollations = new ArrayList<>();
        for (RelFieldCollation inFieldCollation : fieldCollations) {
            int newIndex = targets.getOrDefault(inFieldCollation.getFieldIndex(), Integer.MIN_VALUE);
            if (newIndex == Integer.MIN_VALUE) {
                break;
            } else {
                inFieldCollations.add(inFieldCollation.withFieldIndex(newIndex));
            }
        }

        if (inFieldCollations.size() == fieldCollations.size()) {
            return Pair.of(nodeTraits, List.of(in.replace(RelCollations.of(inFieldCollations))));
        }

        return Pair.of(nodeTraits.replace(RelCollations.EMPTY), List.of(in.replace(RelCollations.EMPTY)));
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The node is rewindable if its input is rewindable.

        RelTraitSet in = inputTraits.get(0);
        RewindabilityTrait rewindability = TraitUtils.rewindability(in);

        return List.of(Pair.of(nodeTraits.replace(rewindability), List.of(in)));
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet in = inputTraits.get(0);

        IgniteDistribution distribution = TraitUtils.projectDistribution(
                TraitUtils.distribution(in), getProjects(), getInput().getRowType());

        return List.of(Pair.of(nodeTraits.replace(distribution), List.of(in)));
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet in = inputTraits.get(0);

        RelCollation collation = TraitUtils.projectCollation(
                TraitUtils.collation(in), getProjects(), getInput().getRowType());

        return List.of(Pair.of(nodeTraits.replace(collation), List.of(in)));
    }

    /**
     * PassThroughCorrelation.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughCorrelation(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(getProjects());
        Set<CorrelationId> traitCorrIds = TraitUtils.correlation(nodeTraits).correlationIds();

        if (!traitCorrIds.containsAll(corrIds)) {
            return null;
        }

        return Pair.of(nodeTraits, List.of(inTraits.get(0).replace(TraitUtils.correlation(nodeTraits))));
    }

    /**
     * DeriveCorrelation.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(getProjects());

        corrIds.addAll(TraitUtils.correlation(inTraits.get(0)).correlationIds());

        return List.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(corrIds)), inTraits));
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(getInput());

        return planner.getCostFactory().makeCost(rowCount, rowCount * IgniteCost.ROW_PASS_THROUGH_COST, 0);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteProject(cluster, getTraitSet(), sole(inputs), getProjects(), getRowType());
    }
}
