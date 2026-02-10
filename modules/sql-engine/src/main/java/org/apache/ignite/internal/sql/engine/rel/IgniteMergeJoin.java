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

import static org.apache.calcite.rel.RelCollations.containsOrderless;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.ignite.internal.sql.engine.externalize.RelInputEx;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * IgniteMergeJoin.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class IgniteMergeJoin extends AbstractIgniteJoin {
    private static final String REL_TYPE_NAME = "MergeJoin";

    /**
     * Collation of a left child. Keep it here to restore after deserialization.
     */
    private final RelCollation leftCollation;

    /**
     * Collation of a right child. Keep it here to restore after deserialization.
     */
    private final RelCollation rightCollation;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteMergeJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType
    ) {
        this(cluster, traitSet, left, right, condition, variablesSet, joinType,
                left.getTraitSet().getCollation(), right.getTraitSet().getCollation());
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteMergeJoin(RelInput input) {
        this(
                input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getInputs().get(0),
                input.getInputs().get(1),
                input.getExpression("condition"),
                Set.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
                input.getEnum("joinType", JoinRelType.class),
                ((RelInputEx) input).getCollation("leftCollation"),
                ((RelInputEx) input).getCollation("rightCollation")
        );
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    private IgniteMergeJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RelCollation leftCollation,
            RelCollation rightCollation
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);

        this.leftCollation = leftCollation;
        this.rightCollation = rightCollation;
    }

    /** {@inheritDoc} */
    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right,
            JoinRelType joinType, boolean semiJoinDone) {
        return new IgniteMergeJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType,
                left.getTraitSet().getCollation(), right.getTraitSet().getCollation());
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteMergeJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
                getVariablesSet(), getJoinType(), leftCollation, rightCollation);
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);
        RelCollation leftCollation = TraitUtils.collation(left);
        RelCollation rightCollation = TraitUtils.collation(right);

        if (containsOrderless(leftCollation, joinInfo.leftKeys)) {
            // preserve left collation
            rightCollation = buildDesiredCollation(joinInfo, leftCollation, true);
        } else if (containsOrderless(rightCollation, joinInfo.rightKeys)) {
            // preserve right collation
            leftCollation = buildDesiredCollation(joinInfo, rightCollation, false);
        } else {
            // generate new collations
            leftCollation = RelCollations.of(joinInfo.leftKeys);
            rightCollation = RelCollations.of(joinInfo.rightKeys);
        }

        RelCollation desiredCollation = leftCollation;

        if (joinType == RIGHT || joinType == FULL) {
            desiredCollation = RelCollations.EMPTY;
        }

        return List.of(
                Pair.of(
                        nodeTraits.replace(desiredCollation),
                        List.of(
                                left.replace(leftCollation),
                                right.replace(rightCollation)
                        )
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
            RelTraitSet required,
            List<RelTraitSet> inputTraits
    ) {
        RelCollation collation = TraitUtils.collation(required);
        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);

        if (joinType == FULL) {
            return defaultCollationPair(required, left, right);
        }

        int leftInputFieldCount = this.left.getRowType().getFieldCount();

        List<Integer> reqKeys = RelCollations.ordinals(collation);
        List<Integer> leftKeys = joinInfo.leftKeys.toIntegerList();
        List<Integer> rightKeys = joinInfo.rightKeys.incr(leftInputFieldCount).toIntegerList();

        ImmutableBitSet reqKeySet = ImmutableBitSet.of(reqKeys);
        ImmutableBitSet leftKeySet = ImmutableBitSet.of(joinInfo.leftKeys);
        ImmutableBitSet rightKeySet = ImmutableBitSet.of(rightKeys);

        RelCollation nodeCollation;
        RelCollation leftCollation;
        RelCollation rightCollation;

        if (reqKeySet.equals(leftKeySet)) {
            if (joinType == RIGHT) {
                return defaultCollationPair(required, left, right);
            }

            nodeCollation = collation;
            leftCollation = collation;
            rightCollation = buildDesiredCollation(joinInfo, leftCollation, true);
        } else if (containsOrderless(leftKeys, collation)) {
            if (joinType == RIGHT) {
                return defaultCollationPair(required, left, right);
            }

            // if sort keys are subset of left join keys, we can extend collations to make sure all join
            // keys are sorted.
            nodeCollation = collation;
            leftCollation = extendCollation(collation, leftKeys);
            rightCollation = buildDesiredCollation(joinInfo, leftCollation, true);
        } else if (containsOrderless(collation, leftKeys) && reqKeys.stream().allMatch(i -> i < leftInputFieldCount)) {
            if (joinType == RIGHT) {
                return defaultCollationPair(required, left, right);
            }

            // if sort keys are superset of left join keys, and left join keys is prefix of sort keys
            // (order not matter), also sort keys are all from left join input.
            nodeCollation = collation;
            leftCollation = collation;
            rightCollation = buildDesiredCollation(joinInfo, leftCollation, true);
        } else if (reqKeySet.equals(rightKeySet)) {
            if (joinType == LEFT) {
                return defaultCollationPair(required, left, right);
            }

            nodeCollation = collation;
            rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
            leftCollation = buildDesiredCollation(joinInfo, rightCollation, false);
        } else if (containsOrderless(rightKeys, collation)) {
            if (joinType == LEFT) {
                return defaultCollationPair(required, left, right);
            }

            nodeCollation = collation;
            rightCollation = RelCollations.shift(extendCollation(collation, rightKeys), -leftInputFieldCount);
            leftCollation = buildDesiredCollation(joinInfo, rightCollation, false);
        } else {
            return defaultCollationPair(required, left, right);
        }

        return Pair.of(
                required.replace(nodeCollation),
                List.of(
                        left.replace(leftCollation),
                        right.replace(rightCollation)
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        double leftCount = mq.getRowCount(getLeft());

        if (Double.isInfinite(leftCount)) {
            return costFactory.makeInfiniteCost();
        }

        double rightCount = mq.getRowCount(getRight());

        if (Double.isInfinite(rightCount)) {
            return costFactory.makeInfiniteCost();
        }

        double rows = leftCount + rightCount;

        return costFactory.makeCost(rows,
                rows * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("leftCollation", leftCollation)
                .item("rightCollation", rightCollation);
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    /**
     * Get collation of a left child.
     */
    public RelCollation leftCollation() {
        return leftCollation;
    }

    /**
     * Get collation of a right child.
     */
    public RelCollation rightCollation() {
        return rightCollation;
    }

    /** Creates pair with default collation for parent and simple yet sufficient collation for children nodes. */
    private Pair<RelTraitSet, List<RelTraitSet>> defaultCollationPair(
            RelTraitSet nodeTraits,
            RelTraitSet leftInputTraits,
            RelTraitSet rightInputTraits
    ) {
        return Pair.of(
                nodeTraits.replace(RelCollations.EMPTY),
                List.of(
                        leftInputTraits.replace(RelCollations.of(joinInfo.leftKeys)),
                        rightInputTraits.replace(RelCollations.of(joinInfo.rightKeys))
                )
        );
    }

    /**
     * This function extends collation by appending new collation fields defined on keys.
     */
    private static RelCollation extendCollation(RelCollation collation, List<Integer> keys) {
        List<RelFieldCollation> fieldsForNewCollation = new ArrayList<>(keys.size());
        fieldsForNewCollation.addAll(collation.getFieldCollations());

        ImmutableBitSet keysBitset = ImmutableBitSet.of(keys);
        ImmutableBitSet colKeysBitset = ImmutableBitSet.of(collation.getKeys());
        ImmutableBitSet exceptBitset = keysBitset.except(colKeysBitset);

        exceptBitset.forEachInt(i -> fieldsForNewCollation.add(new RelFieldCollation(i)));

        return RelCollations.of(fieldsForNewCollation);
    }

    /**
     * The function builds desired collation for the another join branch regarding provided join conditions.
     *
     * <p>It builds valid right collation for the given left collation and vice versa.
     *
     * <p>Note: the function support complex many-to-many conditions, where a source matches multiple targets and vice versa.
     */
    private static RelCollation buildDesiredCollation(JoinInfo joinInfo, RelCollation collation, boolean left2Right) {
        List<RelFieldCollation> source = collation.getFieldCollations();

        // Build index for futher sorting join conditions
        Int2IntMap collationIndex = new Int2IntArrayMap(source.size());
        for (int i = 0; i < source.size(); i++) {
            collationIndex.put(source.get(i).getFieldIndex(), i);
        }

        List<IntPair> conditionPairs = joinInfo.pairs();
        List<IntPair> mapping = new ArrayList<>(conditionPairs.size());

        if (left2Right) {
            for (IntPair pair : conditionPairs) {
                mapping.add(IntPair.of(collationIndex.get(pair.source), pair.target));
            }
        } else { // Build reverse mapping for right-to-left case.
            for (IntPair pair : conditionPairs) {
                mapping.add(IntPair.of(collationIndex.get(pair.target), pair.source));
            }
        }
        // Sort conditions in order of given source collation.
        mapping.sort(IntPair.ORDERING);

        // Build target collation filtering our duplicates, because they don't affect the collation.
        IntPredicate isUnique = new IntArraySet(mapping.size())::add;
        List<RelFieldCollation> target = new ArrayList<>(mapping.size());
        for (IntPair pair : mapping) {
            if (isUnique.test(pair.target)) {
                target.add(source.get(pair.source).withFieldIndex(pair.target));
            }
        }

        return RelCollations.of(target);
    }
}
