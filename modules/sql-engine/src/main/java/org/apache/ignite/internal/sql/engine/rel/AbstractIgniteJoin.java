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

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.ignite.internal.sql.engine.metadata.IgniteMdRowCount.joinRowCount;
import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.random;
import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.ignite.internal.sql.engine.rel.explain.IgniteRelWriter;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * AbstractIgniteJoin.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class AbstractIgniteJoin extends Join implements TraitsAwareIgniteRel {

    /** Override JoinInfo stored in {@link Join} because that joinInfo treats IS NOT DISTINCT FROM as non-equijoin condition. */
    protected final JoinInfo joinInfo;

    protected AbstractIgniteJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
            RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, List.of(), left, right, condition, variablesSet, joinType);

        JoinInfo baseJoinInfo = super.analyzeCondition();

        if (baseJoinInfo.isEqui()) {
            this.joinInfo = baseJoinInfo;
        } else {
            this.joinInfo = JoinInfo.of(left, right, condition);
        }
    }

    /** {@inheritDoc} */
    @Override
    public JoinInfo analyzeCondition() {
        return joinInfo;
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // We preserve left collation since it's translated into a nested loop join with an outer loop
        // over a left edge. The code below checks and projects left collation on an output row type.

        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);

        RelCollation collation = TraitUtils.collation(left);

        // If nulls are possible at left we has to check whether NullDirection.LAST flag is set on sorted fields.
        if (joinType == RIGHT || joinType == JoinRelType.FULL) {
            for (RelFieldCollation field : collation.getFieldCollations()) {
                if (RelFieldCollation.NullDirection.LAST.nullComparison != field.nullDirection.nullComparison) {
                    collation = RelCollations.EMPTY;
                    break;
                }
            }
        }

        RelTraitSet outTraits = nodeTraits.replace(collation);
        RelTraitSet leftTraits = left.replace(collation);
        RelTraitSet rightTraits = right.replace(RelCollations.EMPTY);

        return List.of(Pair.of(outTraits, List.of(leftTraits, rightTraits)));
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);

        List<Pair<RelTraitSet, List<RelTraitSet>>> res = new ArrayList<>();

        IgniteDistribution leftDistr = TraitUtils.distribution(left);
        IgniteDistribution rightDistr = TraitUtils.distribution(right);

        IgniteDistribution left2rightProjectedDistr = leftDistr.apply(buildTransposeMapping(true));
        IgniteDistribution right2leftProjectedDistr = rightDistr.apply(buildTransposeMapping(false));

        RelTraitSet outTraits;
        RelTraitSet leftTraits;
        RelTraitSet rightTraits;

        // Any join is possible with single and broadcast distributions.
        if (leftDistr == broadcast() && rightDistr == broadcast()) {
            outTraits = nodeTraits.replace(broadcast());
            leftTraits = left.replace(broadcast());
            rightTraits = right.replace(broadcast());
        } else {
            outTraits = nodeTraits.replace(single());
            leftTraits = left.replace(single());
            rightTraits = right.replace(single());
        }

        res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));

        if (nullOrEmpty(joinInfo.pairs())) {
            return List.copyOf(res);
        }

        TargetMapping offsetByLeftSize = Commons.targetOffsetMapping(
                this.right.getRowType().getFieldCount(),
                this.left.getRowType().getFieldCount()
        );

        // Re-hashing of ride side to left.
        if (leftDistr.getType() == HASH_DISTRIBUTED && left2rightProjectedDistr != random()) {
            computeHashOutputOptions(nodeTraits, left, leftDistr, right, left2rightProjectedDistr, res, offsetByLeftSize);
        }

        // Re-hashing of left side to right.
        if (rightDistr.getType() == HASH_DISTRIBUTED && right2leftProjectedDistr != random()) {
            computeHashOutputOptions(nodeTraits, left, right2leftProjectedDistr, right, rightDistr, res, offsetByLeftSize);
        }

        // Full re-hashing by join keys. 
        computeHashOutputOptions(
                nodeTraits, left, hash(joinInfo.leftKeys), right, hash(joinInfo.rightKeys), res, offsetByLeftSize
        );

        return List.copyOf(res);
    }

    private void computeHashOutputOptions(
            RelTraitSet nodeCurrent,
            RelTraitSet leftCurrent,
            IgniteDistribution newLeftDistribution,
            RelTraitSet rightCurrent,
            IgniteDistribution newRightDistribution,
            List<Pair<RelTraitSet, List<RelTraitSet>>> res,
            TargetMapping offsetByLeftSize
    ) {
        if (newLeftDistribution.getType() != HASH_DISTRIBUTED 
                || newRightDistribution.getType() != HASH_DISTRIBUTED) {
            // Throw this error explicitly, otherwise we might get incorrect plan which
            // emits wrong result.
            throw new AssertionError("Only hash-based distribution is allowed");
        }

        RelTraitSet leftTraits = leftCurrent.replace(newLeftDistribution);
        RelTraitSet rightTraits = rightCurrent.replace(newRightDistribution);

        RelTraitSet outTraits;

        // Depending on the type of the join, we may produce output distribution based
        // on left keys, right keys, or both.
        if (shouldEmitLeftSideDistribution(joinType)) {
            outTraits = nodeCurrent.replace(newLeftDistribution);
            res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));
        }

        if (shouldEmitRightSideDistribution(joinType)) {
            outTraits = nodeCurrent.replace(newRightDistribution.apply(offsetByLeftSize));
            res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));
        }

        // If we can emit anything meaningful, then emit output with random distribution.
        if (!shouldEmitLeftSideDistribution(joinType) && !shouldEmitRightSideDistribution(joinType)) {
            outTraits = nodeCurrent.replace(random());
            res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));
        }
    }

    private static boolean shouldEmitLeftSideDistribution(JoinRelType joinType) {
        return !joinType.generatesNullsOnLeft();
    }

    private static boolean shouldEmitRightSideDistribution(JoinRelType joinType) {
        return joinType.projectsRight() && !joinType.generatesNullsOnRight();
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // We preserve left collation since it's translated into a nested loop join with an outer loop
        // over a left edge. The code below checks whether a desired collation is possible and requires
        // appropriate collation from the left edge.

        RelCollation collation = TraitUtils.collation(nodeTraits);

        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);

        if (collation.equals(RelCollations.EMPTY)) {
            return Pair.of(nodeTraits,
                    List.of(left.replace(RelCollations.EMPTY), right.replace(RelCollations.EMPTY)));
        }

        if (!projectsLeft(collation)) {
            collation = RelCollations.EMPTY;
        } else if (joinType == RIGHT || joinType == JoinRelType.FULL) {
            for (RelFieldCollation field : collation.getFieldCollations()) {
                if (RelFieldCollation.NullDirection.LAST.nullComparison != field.nullDirection.nullComparison) {
                    collation = RelCollations.EMPTY;
                    break;
                }
            }
        }

        return Pair.of(nodeTraits.replace(collation),
                List.of(left.replace(collation), right.replace(RelCollations.EMPTY)));
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        // There are several rules:
        // 1) any join is possible on broadcast or single distribution
        // 2) hash distributed join is possible when join keys equal to source distribution keys
        // 3) hash and broadcast distributed tables can be joined when join keys equal to hash
        //    distributed table distribution keys and:
        //      3.1) it's a left join and a hash distributed table is at left
        //      3.2) it's a right join and a hash distributed table is at right
        //      3.3) it's an inner join, this case a hash distributed table may be at any side

        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);

        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        RelDistribution.Type distrType = distribution.getType();
        switch (distrType) {
            case BROADCAST_DISTRIBUTED:
            case SINGLETON:
                return Pair.of(nodeTraits, Commons.transform(inputTraits, t -> t.replace(distribution)));

            case HASH_DISTRIBUTED:
            case RANDOM_DISTRIBUTED:
                // Such join may be replaced as a cross join with a filter uppon it.
                // It's impossible to get random or hash distribution from a cross join.
                if (nullOrEmpty(joinInfo.pairs())) {
                    break;
                }

                // We cannot provide random distribution without unique constraint on join keys,
                // so, we require hash distribution (wich satisfies random distribution) instead.
                IgniteDistribution outDistr = distrType == HASH_DISTRIBUTED
                        ? IgniteDistributions.clone(distribution, joinInfo.leftKeys)
                        : hash(joinInfo.leftKeys);

                if (distrType != HASH_DISTRIBUTED || outDistr.satisfies(distribution)) {
                    IgniteDistribution rightDistribution = distrType == HASH_DISTRIBUTED
                            ? IgniteDistributions.clone(distribution, joinInfo.rightKeys)
                            : hash(joinInfo.rightKeys);

                    return Pair.of(nodeTraits.replace(outDistr),
                            List.of(left.replace(outDistr), right.replace(rightDistribution)));
                }

                break;

            default:
                // NO-OP
        }

        return Pair.of(nodeTraits.replace(single()), Commons.transform(inputTraits, t -> t.replace(single())));
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return Util.first(joinRowCount(mq, this), 1D);
    }

    protected boolean projectsLeft(RelCollation collation) {
        int leftFieldCount = getLeft().getRowType().getFieldCount();
        for (int field : RelCollations.ordinals(collation)) {
            if (field >= leftFieldCount) {
                return false;
            }
        }
        return true;
    }

    /** Creates mapping from left join keys to the right and vice versa with regards to {@code left2Right}. */
    protected Mappings.TargetMapping buildTransposeMapping(boolean left2Right) {
        ImmutableIntList sourceKeys = left2Right ? joinInfo.leftKeys : joinInfo.rightKeys;
        ImmutableIntList targetKeys = left2Right ? joinInfo.rightKeys : joinInfo.leftKeys;

        return Mappings.target(
                IntPair.zip(sourceKeys, targetKeys, true),
                (left2Right ? left : right).getRowType().getFieldCount(),
                (left2Right ? right : left).getRowType().getFieldCount()
        );
    }

    @Override
    protected RelDataType deriveRowType() {
        return deriveRowTypeAsFor(joinType);
    }

    @Override
    public IgniteRelWriter explain(IgniteRelWriter writer) {
        return writer
                // Predicate is composed based on joint row type, therefore if original join doesn't projects rhs,
                // then we have to rebuild row type just for predicate.
                .addPredicate(condition, joinType.projectsRight() ? getRowType() : deriveRowTypeAsFor(JoinRelType.INNER))
                .addJoinType(joinType);
    }

    private RelDataType deriveRowTypeAsFor(JoinRelType joinType) {
        List<String> fieldNames = new ArrayList<>(left.getRowType().getFieldNames());

        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();

        if (joinType.projectsRight()) {
            fieldNames.addAll(right.getRowType().getFieldNames());

            fieldNames = SqlValidatorUtil.uniquify(
                    fieldNames,
                    (original, attempt, size) -> original + "$" + attempt,
                    typeFactory.getTypeSystem().isSchemaCaseSensitive()
            );
        }

        return SqlValidatorUtil.deriveJoinRowType(
                left.getRowType(),
                right.getRowType(),
                joinType,
                getCluster().getTypeFactory(),
                fieldNames,
                List.of()
        );
    }
}
