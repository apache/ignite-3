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

package org.apache.ignite.internal.sql.engine.rule.logical;

import static java.lang.Integer.bitCount;
import static org.apache.ignite.internal.util.IgniteUtils.isPow2;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Transformation rule used for optimizing multi-join queries using a bushy join tree strategy.
 *
 * <p>This is an implementation of subset-driven enumeration algorithm (Inspired by G. Moerkotte and T. Neumann.
 * Analysis of Two Existing and One New Dynamic Programming Algorithm for the Generation of Optimal Bushy Join
 * Trees without Cross Products. 2.2 Subset-Driven Enumeration). The main loop enumerates all subsets of relations
 * in a way suitable for dynamic programming: it guarantees, that for every emitted set {@code S}, any split of the
 * {@code S} will produce subsets which have been already processed:
 * <pre>
 *     For example, for join of 4 relations it will produce following sequence: 
 *         0011
 *         0101
 *         0110
 *         ...
 *         1101
 *         1110
 *         1111
 * </pre>
 *
 * <p>The inner while-loop enumerates all possible splits of given subset {@code S} on disjoint subset
 * {@code lhs} and {@code rhs} such that {@code lhs ∪ rhs = S} (Inspired by B. Vance and D. Maier.
 * Rapid bushy join-order optimization with cartesian products).
 *
 * <p>Finally, if the initial set of relations is not connected, the algorithm composes cartesian join
 * from best plans, until all relations are joined.
 *
 * <p>Current limitations are as follow:<ol>
 *     <li>Only INNER joins are supported</li>
 *     <li>Number of relations to optimize is limited to 20. This is due to time and memory complexity of algorithm chosen.</li>
 *     <li>Disjunctive predicate is not considered as connections.</li>
 * </ol>
 */
@Value.Enclosing
public class IgniteMultiJoinOptimizeBushyRule
        extends RelRule<IgniteMultiJoinOptimizeBushyRule.Config>
        implements TransformationRule {

    private static final int MAX_JOIN_SIZE = 20;

    /**
     * Comparator that puts better vertexes first.
     *
     * <p>Better vertex is the one that incorporate more relations, or costs less.
     */
    private static final Comparator<Vertex> VERTEX_COMPARATOR = 
            Comparator.<Vertex>comparingInt(v -> v.size)
                    .reversed()
                    .thenComparingDouble(v -> v.cost);

    /** Creates a MultiJoinOptimizeBushyRule. */
    private IgniteMultiJoinOptimizeBushyRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MultiJoin multiJoinRel = call.rel(0);

        int numberOfRelations = multiJoinRel.getInputs().size();
        if (numberOfRelations > MAX_JOIN_SIZE) {
            return;
        }

        // Currently, algorithm below can handle only INNER JOINs
        if (multiJoinRel.isFullOuterJoin()) {
            return;
        }

        for (JoinRelType joinType : multiJoinRel.getJoinTypes()) {
            if (joinType != JoinRelType.INNER) {
                return;
            }
        }

        LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

        RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        RelBuilder relBuilder = call.builder();
        RelMetadataQuery mq = call.getMetadataQuery();

        List<RexNode> unusedConditions = new ArrayList<>();

        Int2ObjectMap<List<Edge>> edges = collectEdges(multiJoin, unusedConditions);
        Int2ObjectMap<Vertex> bestPlan = new Int2ObjectOpenHashMap<>();
        BitSet connections = new BitSet(1 << numberOfRelations);

        int id = 0b1;
        int fieldOffset = 0;
        for (RelNode input : multiJoinRel.getInputs()) {
            TargetMapping mapping = Mappings.offsetSource(
                    Mappings.createIdentity(input.getRowType().getFieldCount()),
                    fieldOffset,
                    multiJoin.getNumTotalFields()
            );

            bestPlan.put(id, new Vertex(id, mq.getRowCount(input), input, mapping));
            connections.set(id);

            id <<= 1;
            fieldOffset += input.getRowType().getFieldCount();
        }

        Vertex bestSoFar = null;
        for (int s = 0b11; s < 1 << numberOfRelations; s++) {
            if (isPow2(s)) {
                // Single relations have been processed during initialization.
                continue;
            }

            int lhs = Integer.lowestOneBit(s);
            while (lhs < (s / 2) + 1) {
                int rhs = s - lhs;

                List<Edge> edges0;
                if (connections.get(lhs) && connections.get(rhs)) {
                    edges0 = findEdges(lhs, rhs, edges);
                } else {
                    edges0 = List.of();
                }

                if (!edges0.isEmpty()) {
                    connections.set(s);

                    Vertex planLhs = bestPlan.get(lhs);
                    Vertex planRhs = bestPlan.get(rhs);

                    Vertex newPlan = createJoin(planLhs, planRhs, edges0, mq, relBuilder, rexBuilder);
                    Vertex currentBest = bestPlan.get(s);
                    if (currentBest == null || currentBest.cost > newPlan.cost) {
                        bestPlan.put(s, newPlan);

                        bestSoFar = chooseBest(bestSoFar, newPlan);
                    }

                    aggregateEdges(edges, lhs, rhs);
                }

                lhs = s & (lhs - s);
            }
        }

        int allRelationsMask = (1 << numberOfRelations) - 1;

        Vertex best;
        if (bestSoFar == null || bestSoFar.id != allRelationsMask) {
            best = composeCartesianJoin(allRelationsMask, bestPlan, edges, bestSoFar, mq, relBuilder, rexBuilder);
        } else {
            best = bestSoFar;
        }

        RelNode result = relBuilder
                .push(best.rel)
                .filter(RexUtil.composeConjunction(rexBuilder, unusedConditions)
                        .accept(new RexPermuteInputsShuttle(best.mapping, best.rel)))
                .project(relBuilder.fields(best.mapping))
                .build();

        call.transformTo(result);
    }

    private static void aggregateEdges(Int2ObjectMap<List<Edge>> edges, int lhs, int rhs) {
        int id = lhs | rhs;
        if (!edges.containsKey(id)) {
            Set<Edge> used = Collections.newSetFromMap(new IdentityHashMap<>());

            List<Edge> union = new ArrayList<>(edges.getOrDefault(lhs, List.of()));
            used.addAll(union);

            edges.getOrDefault(rhs, List.of()).forEach(edge -> {
                if (used.add(edge)) {
                    union.add(edge);
                }
            });

            if (!union.isEmpty()) {
                edges.put(id, union);
            }
        }
    }

    private static Vertex composeCartesianJoin(
            int allRelationsMask,
            Int2ObjectMap<Vertex> bestPlan,
            Int2ObjectMap<List<Edge>> edges,
            @Nullable Vertex bestSoFar,
            RelMetadataQuery mq,
            RelBuilder relBuilder,
            RexBuilder rexBuilder
    ) {
        List<Vertex> options;

        if (bestSoFar != null) {
            options = new ArrayList<>();

            for (Vertex option : bestPlan.values()) {
                if ((option.id & bestSoFar.id) == 0) {
                    options.add(option);
                }
            }
        } else {
            options = new ArrayList<>(bestPlan.values());
        }

        options.sort(VERTEX_COMPARATOR);

        Iterator<Vertex> it = options.iterator();

        if (bestSoFar == null) {
            bestSoFar = it.next();
        }

        while (it.hasNext() && bestSoFar.id != allRelationsMask) {
            Vertex input = it.next();

            if ((bestSoFar.id & input.id) != 0) {
                continue;
            }

            List<Edge> edges0 = findEdges(bestSoFar.id, input.id, edges);

            aggregateEdges(edges, bestSoFar.id, input.id);

            bestSoFar = createJoin(bestSoFar, input, edges0, mq, relBuilder, rexBuilder);
        }

        assert bestSoFar.id == allRelationsMask;

        return bestSoFar;
    }

    private static Vertex chooseBest(@Nullable Vertex currentBest, Vertex candidate) {
        if (currentBest == null) {
            return candidate;
        }

        if (VERTEX_COMPARATOR.compare(currentBest, candidate) > 0) {
            return candidate;
        }

        return currentBest;
    }

    private static Int2ObjectMap<List<Edge>> collectEdges(LoptMultiJoin multiJoin, List<RexNode> unusedConditions) {
        Int2ObjectMap<List<Edge>> edges = new Int2ObjectOpenHashMap<>();

        for (RexNode condition : multiJoin.getJoinFilters()) {
            int[] inputRefs = multiJoin.getFactorsRefByJoinFilter(condition).toArray();

            // No need to collect conditions involving a single table, because 1) during main loop
            // we will be looking only for edges connecting two subsets, and condition referring to
            // a single table never meet this condition, and 2) for inner join such conditions must
            // be pushed down already, and we rely on this fact.
            if (inputRefs.length < 2) {
                unusedConditions.add(condition);
                continue;
            }

            // TODO: https://issues.apache.org/jira/browse/IGNITE-24210 the whole if-block need to be removed
            if (condition.isA(SqlKind.OR)) {
                unusedConditions.add(condition);
                continue;
            }

            int connectedInputs = 0;
            for (int i : inputRefs) {
                connectedInputs |= 1 << i;
            }

            Edge edge = new Edge(connectedInputs, condition);
            for (int i : inputRefs) {
                edges.computeIfAbsent(1 << i, k -> new ArrayList<>()).add(edge);
            }
        }

        return edges;
    }

    private static Vertex createJoin(
            Vertex lhs,
            Vertex rhs,
            List<Edge> edges,
            RelMetadataQuery metadataQuery,
            RelBuilder relBuilder,
            RexBuilder rexBuilder
    ) {
        List<RexNode> conditions = new ArrayList<>();

        for (Edge e : edges) {
            conditions.add(e.condition);
        }

        double leftSize = metadataQuery.getRowCount(lhs.rel);
        double rightSize = metadataQuery.getRowCount(rhs.rel);

        Vertex majorFactor;
        Vertex minorFactor;

        // Let's put bigger input on left side, because right side will probably be materialized.
        if (leftSize >= rightSize) {
            majorFactor = lhs;
            minorFactor = rhs;
        } else {
            majorFactor = rhs;
            minorFactor = lhs;
        }

        TargetMapping mapping = Mappings.merge(
                majorFactor.mapping,
                Mappings.offsetTarget(
                        minorFactor.mapping,
                        majorFactor.rel.getRowType().getFieldCount()
                )
        );

        RexNode condition = RexUtil.composeConjunction(rexBuilder, conditions)
                .accept(new RexPermuteInputsShuttle(mapping, majorFactor.rel, minorFactor.rel));

        RelNode join = relBuilder
                .push(majorFactor.rel)
                .push(minorFactor.rel)
                .join(JoinRelType.INNER, condition)
                .build();

        double selfCost = metadataQuery.getRowCount(join);

        return new Vertex(lhs.id | rhs.id, selfCost + lhs.cost + rhs.cost, join, mapping);
    }

    /**
     * Finds all edges which connect given subsets.
     * 
     * <p>Returned edges will satisfy two conditions:<ol>
     *     <li>At least one relation from each side will be covered by edge.</li>
     *     <li>No any other relations outside of {@code lhs ∪ rhs} will be covered by edge.</li>
     * </ol>
     *
     * @param lhs Left subset.
     * @param rhs Right subset.
     * @param edges All edges.
     * @return List of edges connecting given subsets.
     */
    private static List<Edge> findEdges(
            int lhs,
            int rhs,
            Int2ObjectMap<List<Edge>> edges
    ) {
        List<Edge> result = new ArrayList<>();
        List<Edge> fromLeft = edges.getOrDefault(lhs, List.of());
        for (Edge edge : fromLeft) {
            int requiredInputs = edge.connectedInputs & ~lhs;
            if (requiredInputs == 0 || edge.connectedInputs == requiredInputs) {
                continue;
            }

            requiredInputs &= ~rhs;
            if (requiredInputs == 0) {
                result.add(edge);
            }
        }

        return result;
    }

    private static class Edge {
        /** Bitmap of all inputs connected by condition. */
        private final int connectedInputs;
        private final RexNode condition;

        Edge(int connectedInputs, RexNode condition) {
            this.connectedInputs = connectedInputs;
            this.condition = condition;
        }
    }

    private static class Vertex {
        /** Bitmap of inputs joined together so far with current vertex served as a root. */
        private final int id;
        /** Number of inputs joined together so far with current vertex served as a root. */
        private final byte size;
        /** Cumulative cost of the tree. */
        private final double cost;
        private final TargetMapping mapping;
        private final RelNode rel;

        Vertex(int id, double cost, RelNode rel, TargetMapping mapping) {
            this.id = id;
            this.size = (byte) bitCount(id);
            this.cost = cost;
            this.rel = rel;
            this.mapping = mapping;
        }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIgniteMultiJoinOptimizeBushyRule.Config.of()
                .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        @Override
        default IgniteMultiJoinOptimizeBushyRule toRule() {
            return new IgniteMultiJoinOptimizeBushyRule(this);
        }
    }
}
