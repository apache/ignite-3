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

package org.apache.ignite.internal.sql.engine.trait;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.plan.RelOptUtil.permutationPushDownProject;
import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.jetbrains.annotations.Nullable;

/**
 * TraitUtils. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TraitUtils {
    /**
     * Enforce. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Nullable
    public static RelNode enforce(RelNode rel, RelTraitSet toTraits) {
        RelOptPlanner planner = rel.getCluster().getPlanner();
        RelTraitSet fromTraits = rel.getTraitSet();
        int size = Math.min(fromTraits.size(), toTraits.size());

        if (!fromTraits.satisfies(toTraits)) {
            RelNode old = null;

            for (int i = 0; rel != null && i < size; i++) {
                RelTrait fromTrait = rel.getTraitSet().getTrait(i);
                RelTrait toTrait = toTraits.getTrait(i);

                if (fromTrait.satisfies(toTrait)) {
                    continue;
                }

                if (old != null && old != rel) {
                    rel = planner.register(rel, old);
                }

                old = rel;

                rel = convertTrait(planner, fromTrait, toTrait, rel);

                assert rel == null || rel.getTraitSet().getTrait(i).satisfies(toTrait);
            }

            assert rel == null || rel.getTraitSet().satisfies(toTraits);
        }

        return rel;
    }

    @SuppressWarnings({"rawtypes"})
    @Nullable
    private static RelNode convertTrait(RelOptPlanner planner, RelTrait fromTrait, RelTrait toTrait, RelNode rel) {
        assert fromTrait.getTraitDef() == toTrait.getTraitDef();

        RelTraitDef converter = fromTrait.getTraitDef();

        if (rel.getConvention() != IgniteConvention.INSTANCE) {
            return null;
        }

        if (converter == RelCollationTraitDef.INSTANCE) {
            return convertCollation(planner, (RelCollation) toTrait, rel);
        } else if (converter == DistributionTraitDef.INSTANCE) {
            return convertDistribution(planner, (IgniteDistribution) toTrait, rel);
        } else {
            return convertOther(planner, converter, toTrait, rel);
        }
    }

    /**
     * Convert collation. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Nullable
    public static RelNode convertCollation(RelOptPlanner planner,
            RelCollation toTrait, RelNode rel) {
        RelCollation fromTrait = collation(rel);

        if (fromTrait.satisfies(toTrait)) {
            return rel;
        }

        RelTraitSet traits = rel.getTraitSet().replace(toTrait);

        return new IgniteSort(rel.getCluster(), traits, rel, toTrait);
    }

    /**
     * Convert distribution. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Nullable
    public static RelNode convertDistribution(RelOptPlanner planner,
            IgniteDistribution toTrait, RelNode rel) {
        IgniteDistribution fromTrait = distribution(rel);

        if (fromTrait.satisfies(toTrait)) {
            return rel;
        }

        RelTraitSet traits = rel.getTraitSet().replace(toTrait);
        if (fromTrait.getType() == BROADCAST_DISTRIBUTED && toTrait.getType() == HASH_DISTRIBUTED) {
            return new IgniteTrimExchange(rel.getCluster(), traits, rel, toTrait);
        } else {
            if (toTrait.getType() != SINGLETON && collation(traits) != RelCollations.EMPTY) {
                return null;
            }

            return new IgniteExchange(
                    rel.getCluster(),
                    traits,
                    RelOptRule.convert(
                            rel,
                            rel.getTraitSet()
                    ),
                    toTrait);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Nullable
    private static RelNode convertOther(RelOptPlanner planner, RelTraitDef converter,
            RelTrait toTrait, RelNode rel) {
        RelTrait fromTrait = rel.getTraitSet().getTrait(converter);

        if (fromTrait.satisfies(toTrait)) {
            return rel;
        }

        if (!converter.canConvert(planner, fromTrait, toTrait)) {
            return null;
        }

        return converter.convert(planner, rel, toTrait, true);
    }

    /**
     * Distribution. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static IgniteDistribution distribution(RelNode rel) {
        return rel instanceof IgniteRel
                ? ((IgniteRel) rel).distribution()
                : distribution(rel.getTraitSet());
    }

    /**
     * Distribution. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static IgniteDistribution distribution(RelTraitSet traits) {
        return traits.getTrait(DistributionTraitDef.INSTANCE);
    }

    /**
     * Check distribution definition in traits.
     *
     * @param traitDefs Traits to analyze.
     * @return {@code true} if distribution found, {@code false} otherwise.
     */
    public static boolean distributionPresent(ImmutableList<RelTraitDef> traitDefs) {
        for (RelTraitDef<?> trait : traitDefs) {
            if (trait instanceof DistributionTraitDef) {
                return true;
            }
        }
        return false;
    }

    /**
     * Collation. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RelCollation collation(RelNode rel) {
        return rel instanceof IgniteRel
                ? ((IgniteRel) rel).collation()
                : collation(rel.getTraitSet());
    }

    /**
     * Collation. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RelCollation collation(RelTraitSet traits) {
        return traits.getTrait(RelCollationTraitDef.INSTANCE);
    }

    /**
     * ChangeTraits. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RelInput changeTraits(RelInput input, RelTrait... traits) {
        RelTraitSet traitSet = input.getTraitSet();

        for (RelTrait trait : traits) {
            traitSet = traitSet.replace(trait);
        }

        RelTraitSet traitSet0 = traitSet;

        return (RelInput) Proxy.newProxyInstance(TraitUtils.class.getClassLoader(), input.getClass().getInterfaces(), (p, m, a) -> {
            if ("getTraitSet".equals(m.getName())) {
                return traitSet0;
            }

            return m.invoke(input, a);
        });
    }

    /**
     * ProjectCollation. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RelCollation projectCollation(RelCollation collation, List<RexNode> projects, RelDataType inputRowType) {
        if (collation.getFieldCollations().isEmpty()) {
            return RelCollations.EMPTY;
        }

        Mappings.TargetMapping mapping = permutationPushDownProject(projects, inputRowType, 0, 0);

        return collation.apply(mapping);
    }

    /**
     * ProjectDistribution. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static IgniteDistribution projectDistribution(IgniteDistribution distribution, List<RexNode> projects,
            RelDataType inputRowType) {
        if (distribution.getType() != HASH_DISTRIBUTED) {
            return distribution;
        }

        Mappings.TargetMapping mapping = createProjectionMapping(inputRowType.getFieldCount(), projects);

        return distribution.apply(mapping);
    }

    /**
     * PassThrough. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Pair<RelTraitSet, List<RelTraitSet>> passThrough(TraitsAwareIgniteRel rel, RelTraitSet requiredTraits) {
        return passThrough(IgniteConvention.INSTANCE, rel, requiredTraits);
    }

    /**
     * PassThrough. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Pair<RelTraitSet, List<RelTraitSet>> passThrough(Convention convention, TraitsAwareIgniteRel rel,
            RelTraitSet requiredTraits) {
        if (requiredTraits.getConvention() != convention || rel.getInputs().isEmpty()) {
            return null;
        }

        List<RelTraitSet> inTraits = Collections.nCopies(rel.getInputs().size(),
                rel.getCluster().traitSetOf(convention));

        PropagationContext context = new PropagationContext(Set.of(Pair.of(requiredTraits, inTraits)))
                .propagate((in, outs) -> singletonListFromNullable(rel.passThroughCollation(in, outs)));

        if (distributionEnabled(rel)) {
            context = context.propagate((in, outs) -> singletonListFromNullable(rel.passThroughDistribution(in, outs)));
        }

        List<Pair<RelTraitSet, List<RelTraitSet>>> traits = context.combinations();

        assert traits.size() <= 1;

        return first(traits);
    }

    /**
     * Derive. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static List<RelNode> derive(TraitsAwareIgniteRel rel, List<List<RelTraitSet>> inTraits) {
        return derive(IgniteConvention.INSTANCE, rel, inTraits);
    }

    /**
     * Derive. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static List<RelNode> derive(Convention convention, TraitsAwareIgniteRel rel, List<List<RelTraitSet>> inTraits) {
        assert !nullOrEmpty(inTraits);

        if (inTraits.stream().flatMap(List::stream).anyMatch(traitSet -> traitSet.getConvention() != convention)) {
            return List.of();
        }

        RelTraitSet outTraits = rel.getCluster().traitSetOf(convention);
        Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations = combinations(outTraits, inTraits);

        if (combinations.isEmpty()) {
            return List.of();
        }

        PropagationContext context = new PropagationContext(combinations)
                .propagate(rel::deriveCollation);

        if (distributionEnabled(rel)) {
            context = context.propagate(rel::deriveDistribution);
        }

        return context.nodes(rel::createNode);
    }

    /**
     * SingletonListFromNullable. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param elem Elem.
     */
    private static <T> List<T> singletonListFromNullable(@Nullable T elem) {
        return elem == null ? emptyList() : singletonList(elem);
    }

    private static Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations(RelTraitSet outTraits, List<List<RelTraitSet>> inTraits) {
        Set<Pair<RelTraitSet, List<RelTraitSet>>> out = new HashSet<>();
        fillRecursive(outTraits, inTraits, out, new RelTraitSet[inTraits.size()], 0);
        return out;
    }

    private static boolean fillRecursive(
            RelTraitSet outTraits,
            List<List<RelTraitSet>> inTraits,
            Set<Pair<RelTraitSet, List<RelTraitSet>>> result,
            RelTraitSet[] combination,
            int idx
    ) throws ControlFlowException {
        boolean processed = false;
        boolean last = idx == inTraits.size() - 1;
        for (RelTraitSet t : inTraits.get(idx)) {
            // change into: assert t.getConvention() == IgniteConvention.INSTANCE; after the ExternalConvention(EXTENSION_NAME is removed.
            if (t.getConvention() != IgniteConvention.INSTANCE) {
                continue;
            }

            processed = true;
            combination[idx] = t;

            if (last) {
                result.add(Pair.of(outTraits, List.of(combination)));
            } else if (!fillRecursive(outTraits, inTraits, result, combination, idx + 1)) {
                return false;
            }
        }
        return processed;
    }

    /**
     * Creates collations from provided keys.
     *
     * @param keys The keys to create collation from.
     * @return New collation.
     */
    public static RelCollation createCollation(Collection<Integer> keys) {
        return RelCollations.of(
                keys.stream().map(TraitUtils::createFieldCollation).collect(Collectors.toList())
        );
    }

    /**
     * Creates {@link RelCollation} object from a given collations.
     *
     * @param collations List of collations.
     * @return a {@link RelCollation} object.
     */
    public static RelCollation createCollation(List<Collation> collations) {
        List<RelFieldCollation> fieldCollations = new ArrayList<>(collations.size());

        for (int i = 0; i < collations.size(); i++) {
            fieldCollations.add(createFieldCollation(i,  collations.get(i)));
        }

        return RelCollations.of(fieldCollations);
    }

    /**
     * Creates {@link RelCollation} object from a given collations.
     *
     * @param indexedColumns List of columns names.
     * @param collations List of collations.
     * @param descriptor Table descriptor to derive column indexes from.
     * @return a {@link RelCollation} object.
     */
    public static RelCollation createCollation(
            List<String> indexedColumns,
            @Nullable List<IgniteIndex.Collation> collations,
            TableDescriptor descriptor
    ) {
        List<RelFieldCollation> fieldCollations = new ArrayList<>(indexedColumns.size());

        if (collations == null) { // Build collation for Hash index.
            for (int i = 0; i < indexedColumns.size(); i++) {
                String columnName = indexedColumns.get(i);
                ColumnDescriptor columnDesc = descriptor.columnDescriptor(columnName);

                fieldCollations.add(new RelFieldCollation(columnDesc.logicalIndex(), Direction.CLUSTERED, NullDirection.UNSPECIFIED));
            }

            return RelCollations.of(fieldCollations);
        }

        for (int i = 0; i < indexedColumns.size(); i++) {
            String columnName = indexedColumns.get(i);
            IgniteIndex.Collation collation = collations.get(i);
            ColumnDescriptor columnDesc = descriptor.columnDescriptor(columnName);

            fieldCollations.add(createFieldCollation(columnDesc.logicalIndex(), collation));
        }

        return RelCollations.of(fieldCollations);
    }

    /**
     * Creates field collation with default direction and nulls ordering.
     */
    public static RelFieldCollation createFieldCollation(int fieldIdx) {
        return new RelFieldCollation(fieldIdx, Direction.ASCENDING, NullDirection.LAST);
    }

    /** Creates field collation. */
    public static RelFieldCollation createFieldCollation(int fieldIdx, IgniteIndex.Collation collation) {
        switch (collation) {
            case ASC_NULLS_LAST:
                return new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST);
            case ASC_NULLS_FIRST:
                return new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST);
            case DESC_NULLS_LAST:
                return new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST);
            case DESC_NULLS_FIRST:
                return new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.FIRST);
            default:
                throw new IgniteInternalException(Common.INTERNAL_ERR, format("Unknown collation [collation={}]", collation));
        }
    }

    public static boolean distributionEnabled(RelNode node) {
        return distribution(node) != null;
    }

    /**
     * Creates mapping from provided projects that maps a source column idx to idx in a row after applying projections.
     *
     * @param inputFieldCount Size of a source row.
     * @param projects        Projections.
     */
    private static Mappings.TargetMapping createProjectionMapping(int inputFieldCount, List<? extends RexNode> projects) {
        Int2IntOpenHashMap src2target = new Int2IntOpenHashMap();

        for (Ord<RexNode> exp : Ord.<RexNode>zip(projects)) {
            if (exp.e instanceof RexInputRef) {
                src2target.putIfAbsent(((RexSlot) exp.e).getIndex(), exp.i);
            }
        }

        return Mappings.target(src -> src2target.getOrDefault(src, -1), inputFieldCount, projects.size());
    }

    private static class PropagationContext {
        private final Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations;

        private PropagationContext(Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations) {
            this.combinations = combinations;
        }

        /**
         * Propagates traits in bottom-up or up-to-bottom manner using given traits propagator.
         */
        public PropagationContext propagate(TraitsPropagator processor) {
            if (combinations.isEmpty()) {
                return this;
            }

            Set<Pair<RelTraitSet, List<RelTraitSet>>> b = new HashSet<>();

            for (Pair<RelTraitSet, List<RelTraitSet>> variant : combinations) {
                b.addAll(processor.propagate(variant.left, variant.right));
            }

            return new PropagationContext(Set.copyOf(b));
        }

        /**
         * Creates nodes using given factory.
         */
        public List<RelNode> nodes(RelFactory nodesCreator) {
            if (combinations.isEmpty()) {
                return List.of();
            }

            List<RelNode> nodes = new ArrayList<>();

            for (Pair<RelTraitSet, List<RelTraitSet>> variant : combinations) {
                nodes.add(nodesCreator.create(variant.left, variant.right));
            }

            return List.copyOf(nodes);
        }

        public List<Pair<RelTraitSet, List<RelTraitSet>>> combinations() {
            return List.copyOf(combinations);
        }
    }

    private interface TraitsPropagator {
        /**
         * Propagates traits in bottom-up or up-to-bottom manner.
         *
         * @param outTraits Relational node traits.
         * @param inTraits  Relational node input traits.
         * @return List of possible input-output traits combinations.
         */
        List<Pair<RelTraitSet, List<RelTraitSet>>> propagate(RelTraitSet outTraits, List<RelTraitSet> inTraits);
    }
}
