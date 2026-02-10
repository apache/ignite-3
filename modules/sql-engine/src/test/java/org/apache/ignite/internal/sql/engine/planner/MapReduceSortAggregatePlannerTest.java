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

import static java.util.function.Predicate.not;
import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.util.ArrayUtils;
import org.junit.jupiter.api.Test;

/**
 * This test verifies that queries defined in {@link TestCase TestCase} can be optimized with usage of
 * 2 phase sort aggregates only.
 *
 * <p>See {@link AbstractAggregatePlannerTest base class} for more details.
 */
public class MapReduceSortAggregatePlannerTest extends AbstractAggregatePlannerTest {
    private final String[] disableRules = {
            "MapReduceHashAggregateConverterRule",
            "ColocatedHashAggregateConverterRule",
            "ColocatedSortAggregateConverterRule"
    };

    /**
     * Validates a plan for simple query with aggregate.
     */
    @Test
    protected void simpleAggregate() throws Exception {
        checkSimpleAggSingle(TestCase.CASE_1);
        checkSimpleAggHash(TestCase.CASE_1A);
        checkSimpleAggHash(TestCase.CASE_1B);
    }

    /**
     * Validates a plan for simple query with DISTINCT aggregates.
     *
     * @see #minMaxDistinctAggregate()
     */
    @Test
    public void distinctAggregate() throws Exception {
        checkDistinctAggSingle(TestCase.CASE_2_1);
        checkDistinctAggSingle(TestCase.CASE_2_2);

        checkDistinctAggHash(TestCase.CASE_2_1A);
        checkDistinctAggHash(TestCase.CASE_2_2A);

        checkDistinctAggHash(TestCase.CASE_2_1B);
        checkDistinctAggHash(TestCase.CASE_2_2B);

        checkDistinctAggHash(TestCase.CASE_2_1C);
        checkDistinctAggHash(TestCase.CASE_2_2C);

        checkDistinctAggHash(TestCase.CASE_2_1D);
        checkDistinctAggHash(TestCase.CASE_2_2D);
    }

    /**
     * Validates a plan for a query with min/max distinct aggregate.
     *
     * <p>NB: DISTINCT make no sense for MIN/MAX, thus expected plan is the same as in {@link #simpleAggregate()} ()}
     */
    @Test
    public void minMaxDistinctAggregate() throws Exception {
        checkSimpleAggSingle(TestCase.CASE_3_1);
        checkSimpleAggSingle(TestCase.CASE_3_2);

        checkSimpleAggHash(TestCase.CASE_3_1A);
        checkSimpleAggHash(TestCase.CASE_3_2A);

        checkSimpleAggHash(TestCase.CASE_3_1B);
        checkSimpleAggHash(TestCase.CASE_3_2B);

        checkSimpleAggHash(TestCase.CASE_3_1C);
        checkSimpleAggHash(TestCase.CASE_3_2C);

        checkSimpleAggHash(TestCase.CASE_3_1D);
        checkSimpleAggHash(TestCase.CASE_3_2D);
    }

    /**
     * Validates a plan for a query with aggregate and groups.
     */
    @Test
    public void simpleAggregateWithGroupBy() throws Exception {
        checkSimpleAggWithGroupBySingle(TestCase.CASE_5);
        checkSimpleAggWithGroupBySingle(TestCase.CASE_6);

        checkSimpleAggWithGroupByHash(TestCase.CASE_5A);
        checkSimpleAggWithGroupByHash(TestCase.CASE_6A);

        checkSimpleAggWithGroupByHash(TestCase.CASE_5B);
        checkSimpleAggWithGroupByHash(TestCase.CASE_6B);

        checkSimpleAggWithGroupByHash(TestCase.CASE_5C);
        checkSimpleAggWithGroupByHash(TestCase.CASE_6C);

        checkSimpleAggWithGroupByHash(TestCase.CASE_5D);
    }

    /**
     * Validates a plan for a query with DISTINCT aggregates and groups.
     *
     * @see #minMaxDistinctAggregateWithGroupBy()
     */
    @Test
    public void distinctAggregateWithGroups() throws Exception {
        checkDistinctAggWithGroupBySingle(TestCase.CASE_7_1);
        checkDistinctAggWithGroupBySingle(TestCase.CASE_7_2);
        checkDistinctAggWithGroupBySingle(TestCase.CASE_7_3);

        checkDistinctAggWithGroupByHash(TestCase.CASE_7_1A);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_2A);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_3A);

        checkDistinctAggWithGroupByHash(TestCase.CASE_7_1B);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_2B);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_3B);

        checkDistinctAggWithGroupByHash(TestCase.CASE_7_1C);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_2C);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_3C);

        checkDistinctAggWithGroupByHash(TestCase.CASE_7_1D);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_2D);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_3D);
    }

    /**
     * Validates a plan for a query with min/max distinct aggregates and groups.
     *
     * <p>NB: DISTINCT make no sense for MIN/MAX, thus expected plan is the same as in {@link #simpleAggregateWithGroupBy()}
     */
    @Test
    public void minMaxDistinctAggregateWithGroupBy() throws Exception {
        checkSimpleAggWithGroupBySingle(TestCase.CASE_8_1);
        checkSimpleAggWithGroupBySingle(TestCase.CASE_8_2);

        checkSimpleAggWithGroupByHash(TestCase.CASE_8_1A);
        checkSimpleAggWithGroupByHash(TestCase.CASE_8_2A);

        checkSimpleAggWithGroupByHash(TestCase.CASE_8_1B);
        checkSimpleAggWithGroupByHash(TestCase.CASE_8_2B);

        checkSimpleAggWithGroupByHash(TestCase.CASE_8_1C);
        checkSimpleAggWithGroupByHash(TestCase.CASE_8_2C);

        checkSimpleAggWithGroupByHash(TestCase.CASE_8_1D);
        checkSimpleAggWithGroupByHash(TestCase.CASE_8_2D);
    }

    /**
     * Validates a plan uses an index for a query with aggregate if grouped by index prefix.
     *
     * <p>NB: GROUP BY columns order permutation shouldn't affect the plan.
     */
    @Test
    public void aggregateWithGroupByIndexPrefixColumns() throws Exception {
        checkAggWithGroupByIndexColumnsSingle(TestCase.CASE_9);
        checkAggWithGroupByIndexColumnsSingle(TestCase.CASE_10);
        checkAggWithGroupByIndexColumnsSingle(TestCase.CASE_11);

        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_9A);
        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_10A);
        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_11A);

        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_9B);
        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_10B);
        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_11B);

        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_9C);
        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_10C);
        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_11C);

        checkAggWithGroupByIndexColumnsHash(TestCase.CASE_9D);
    }

    /**
     * Validates a plan for a query with DISTINCT and without aggregation function.
     */
    @Test
    public void distinctWithoutAggregate() throws Exception {
        checkGroupWithNoAggregateSingle(TestCase.CASE_12);

        checkGroupWithNoAggregateHash(TestCase.CASE_12A);
        checkGroupWithNoAggregateHash(TestCase.CASE_12B);
        checkGroupWithNoAggregateHash(TestCase.CASE_12C);
        checkGroupWithNoAggregateHash(TestCase.CASE_12D);
    }

    /**
     * Validates a plan uses index for a query with DISTINCT and without aggregation functions.
     */
    @Test
    public void distinctWithoutAggregateUseIndex() throws Exception {
        checkGroupWithNoAggregateUseIndexSingle(TestCase.CASE_13);

        checkGroupWithNoAggregateUseIndexHash(TestCase.CASE_13A);
        checkGroupWithNoAggregateUseIndexHash(TestCase.CASE_13B);

        checkGroupWithNoAggregateUseIndexHash(TestCase.CASE_13C);
        checkGroupWithNoAggregateUseIndexHash(TestCase.CASE_13D);
    }

    /**
     * Validates a plan for a query which WHERE clause contains a sub-query with aggregate.
     */
    @Test
    public void subqueryWithAggregateInWhereClause() throws Exception {
        checkSimpleAggSingle(TestCase.CASE_14);
        checkSimpleAggHash(TestCase.CASE_14A);
        checkSimpleAggHash(TestCase.CASE_14B);
    }

    /**
     * Validates that the SINGLE_VALUE aggregate is added for a sub-query where a single value is expected.
     */
    @Test
    public void subqueryWithSingleValueAggregate() throws Exception {
        checkSimpleAggSingle(TestCase.CASE_27, hasSingleValueAggregate());
        checkSimpleAggSingle(TestCase.CASE_27A, hasSingleValueAggregate());
        checkSimpleAggSingle(TestCase.CASE_27B, hasSingleValueAggregate());
        checkSimpleAggSingle(TestCase.CASE_27C, hasSingleValueAggregate());
    }

    /**
     * Validates a plan for a query with DISTINCT aggregate in WHERE clause.
     */
    @Test
    public void distinctAggregateInWhereClause() throws Exception {
        checkGroupWithNoAggregateSingle(TestCase.CASE_15);
        checkGroupWithNoAggregateHash(TestCase.CASE_15A);
        checkGroupWithNoAggregateHash(TestCase.CASE_15B);
    }

    /**
     * Validates a plan with merge-sort utilizes index if collation fits.
     */
    @Test
    public void noSortAppendingWithCorrectCollation() throws Exception {
        String[] additionalRulesToDisable = {"NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "CorrelateToNestedLoopRule",
                "HashJoinConverter"};

        assertPlan(TestCase.CASE_16,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(input(isIndexScan("TEST", "idx_val0")))
                        ))
                ),
                ArrayUtils.concat(disableRules, additionalRulesToDisable)
        );

        assertPlan(TestCase.CASE_16A,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isIndexScan("TEST", "idx_val0")))
                                ))
                        ))
                ),
                ArrayUtils.concat(disableRules, additionalRulesToDisable)
        );
        assertPlan(TestCase.CASE_16B,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isIndexScan("TEST", "idx_val0")))
                                ))
                        ))
                ),
                ArrayUtils.concat(disableRules, additionalRulesToDisable)
        );
    }

    /**
     * Validates a plan for a sub-query with order and limit.
     */
    @Test
    public void emptyCollationPassThroughLimit() throws Exception {
        assertPlan(TestCase.CASE_17,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                ),
                disableRules
        );

        assertPlan(TestCase.CASE_17A,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteLimit.class)
                                                .and(input(isInstanceOf(IgniteExchange.class)
                                                        .and(hasDistribution(single()))
                                                        .and(input(isInstanceOf(IgniteSort.class)
                                                                .and(input(isTableScan("TEST")))
                                                        ))
                                                ))
                                        ))
                                ))
                        ))
                ),
                disableRules
        );
        assertPlan(TestCase.CASE_17B,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteLimit.class)
                                                .and(input(isInstanceOf(IgniteExchange.class)
                                                        .and(hasDistribution(single()))
                                                        .and(input(isInstanceOf(IgniteSort.class)
                                                                .and(input(isTableScan("TEST")))
                                                        ))
                                                ))
                                        ))
                                ))
                        ))
                ),
                disableRules
        );
    }

    /**
     * Validates a plan for a query with aggregate and with groups and sorting by the same column set.
     */
    @Test
    public void groupsWithOrderByGroupColumns() throws Exception {
        checkGroupsWithOrderBySubsetOfGroupColumnsSingle(TestCase.CASE_18_1, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderBySubsetOfGroupColumnsSingle(TestCase.CASE_18_2, TraitUtils.createCollation(List.of(1, 0)));
        checkGroupsWithOrderBySubsetOfGroupColumnsSingle(TestCase.CASE_18_3, TraitUtils.createCollation(List.of(1, 0)));

        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_18_1A, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_18_2A, TraitUtils.createCollation(List.of(1, 0)));
        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_18_3A, TraitUtils.createCollation(List.of(1, 0)));

        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_18_1B, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_18_2B, TraitUtils.createCollation(List.of(1, 0)));
        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_18_3B, TraitUtils.createCollation(List.of(1, 0)));
    }

    /**
     * Validates a plan for a query with aggregate and with sorting by subset of group columns.
     */
    @Test
    public void aggregateWithOrderBySubsetOfGroupColumns() throws Exception {
        checkGroupsWithOrderBySubsetOfGroupColumnsSingle(TestCase.CASE_19_1, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderBySubsetOfGroupColumnsSingle(TestCase.CASE_19_2, TraitUtils.createCollation(List.of(1, 0)));

        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_19_1A, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_19_2A, TraitUtils.createCollation(List.of(1, 0)));

        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_19_1B, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_19_2B, TraitUtils.createCollation(List.of(1, 0)));
    }

    /**
     * Validates a plan for a query with aggregate and with group by subset of sort columns.
     */
    @Test
    public void aggregateWithGroupBySubsetOfSortColumns() throws Exception {
        checkGroupsWithGroupBySubsetOfSortColumnsSingle(TestCase.CASE_20, TraitUtils.createCollation(List.of(0, 1)));

        checkGroupsWithGroupBySubsetOfSortColumnsHash(TestCase.CASE_20A, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithGroupBySubsetOfSortColumnsHash(TestCase.CASE_20B, TraitUtils.createCollation(List.of(0, 1)));
    }

    /**
     * Validates a plan for a query with aggregate and groups, and EXPAND_DISTINCT_AGG hint.
     */
    @Test
    public void expandDistinctAggregates() throws Exception {
        Predicate<? extends RelNode> subtreePredicate = isInstanceOf(IgniteReduceSortAggregate.class)
                // Check the second aggregation step contains accumulators.
                // Plan must not contain distinct accumulators.
                .and(hasAggregate())
                .and(not(hasDistinctAggregate()))
                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                        // Check the first aggregation step is SELECT DISTINCT (doesn't contain any accumulators)
                        .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                ))
                        ))
                ));

        assertPlan(TestCase.CASE_21, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ), disableRules);

        subtreePredicate = isInstanceOf(IgniteReduceSortAggregate.class)
                // Check the second aggregation step contains accumulators.
                // Plan must not contain distinct accumulators.
                .and(hasAggregate())
                .and(not(hasDistinctAggregate()))
                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                        // Check the first aggregation step is SELECT DISTINCT (doesn't contain any accumulators)
                        .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(hasDistribution(single()))
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(not(hasAggregate()))
                                                .and(hasGroups())
                                        ))
                                ))
                        ))
                ));

        assertPlan(TestCase.CASE_21A, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ), disableRules);
        assertPlan(TestCase.CASE_21B, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ), disableRules);
    }

    /**
     * Validates that COUNT aggregate is split into COUNT and COUNT_REDUCE.
     */
    @Test
    public void twoPhaseCountAgg() throws Exception {
        Predicate<AggregateCall> countMap = (a) ->
                Objects.equals(a.getAggregation().getName(), "COUNT") && a.getArgList().equals(List.of(1));

        Predicate<AggregateCall> countReduce = (a) ->
                Objects.equals(a.getAggregation().getName(), "$SUM0") && a.getArgList().equals(List.of(1));

        Predicate<RelNode> nonColocated = hasChildThat(isInstanceOf(IgniteReduceSortAggregate.class)
                .and(in -> hasAggregates(countReduce).test(in.getAggregateCalls()))
                .and(input(isInstanceOf(IgniteExchange.class)
                        .and(hasDistribution(single()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(in -> hasAggregates(countMap).test(in.getAggCallList()))
                                )
                        ))
                ));

        assertPlan(TestCase.CASE_22, nonColocated, disableRules);
        assertPlan(TestCase.CASE_22A, nonColocated, disableRules);
        assertPlan(TestCase.CASE_22B, nonColocated, disableRules);
        assertPlan(TestCase.CASE_22C, nonColocated, disableRules);
    }

    /**
     * Validates that AVG aggregate is split into multiple expressions:
     * SUM(col) as s, COUNT(col) as c on map phase and then SUM(s)/SUM0(c) on reduce phase.
     */
    @Test
    public void twoPhaseAvgAgg() throws Exception {
        Predicate<AggregateCall> sumMap = (a) ->
                Objects.equals(a.getAggregation().getName(), "SUM") && a.getArgList().equals(List.of(1));

        Predicate<AggregateCall> countMap = (a) ->
                Objects.equals(a.getAggregation().getName(), "COUNT") && a.getArgList().equals(List.of(1));

        Predicate<AggregateCall> sumReduce = (a) ->
                Objects.equals(a.getAggregation().getName(), "SUM") && a.getArgList().equals(List.of(1));

        Predicate<AggregateCall> sum0Reduce = (a) ->
                Objects.equals(a.getAggregation().getName(), "$SUM0") && a.getArgList().equals(List.of(2));

        Predicate<RelNode> nonColocated = hasChildThat(isInstanceOf(IgniteReduceSortAggregate.class)
                .and(in -> hasAggregates(sumReduce, sum0Reduce).test(in.getAggregateCalls()))
                .and(input(isInstanceOf(IgniteProject.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                        .and(hasDistribution(single()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(in -> hasAggregates(sumMap, countMap).test(in.getAggCallList()))
                                )
                        ))
                ))));

        assertPlan(TestCase.CASE_23, nonColocated, disableRules);
        assertPlan(TestCase.CASE_23A, nonColocated, disableRules);
        assertPlan(TestCase.CASE_23B, nonColocated, disableRules);
        assertPlan(TestCase.CASE_23C, nonColocated, disableRules);
    }

    /**
     * Validates a plan for a query with two aggregates: one w/o DISTINCT and one with DISTINCT: single distribution.
     */
    @Test
    public void countDistinctGroupSetSingle() throws Exception {
        Predicate<IgniteReduceSortAggregate> inputAgg = isInstanceOf(IgniteReduceSortAggregate.class)
                .and(hasGroupSets(IgniteReduceSortAggregate::getGroupSets, 0))
                .and(hasCollation(RelCollations.of(0)))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                        .and(hasCollation(RelCollations.of(1)))
                        .and(hasGroupSets(Aggregate::getGroupSets, 1))
                ));

        assertPlan(TestCase.CASE_24_1, nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasNoGroupSets(IgniteReduceSortAggregate::getGroupSets))
                        .and(hasCollation(RelCollations.EMPTY))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasNoGroupSets(IgniteMapSortAggregate::getGroupSets))
                                .and(hasCollation(RelCollations.EMPTY))
                                .and(input(isInstanceOf(IgniteProject.class).and(input(inputAgg)))
                                ))
                        )),
                disableRules);
    }

    /**
     * Validates a plan for a query with two aggregates: one w/o DISTINCT and one with DISTINCT: hash distribution.
     */
    @Test
    public void countDistinctGroupSetHash() throws Exception {
        checkCountDistinctHash(TestCase.CASE_24_1A);
        checkCountDistinctHash(TestCase.CASE_24_1B);
        checkCountDistinctHash(TestCase.CASE_24_1C);
        checkCountDistinctHash(TestCase.CASE_24_1D);
        checkCountDistinctHash(TestCase.CASE_24_1E);
    }

    /**
     * Validates a plan for a query with aggregate and with groups and sorting by the same column set in descending order.
     */
    @Test
    public void groupsWithOrderByGroupColumnDescending() throws Exception {
        checkDerivedCollationWithOrderByGroupColumnSingle(TestCase.CASE_25);

        checkDerivedCollationWithOrderByGroupColumnHash(TestCase.CASE_25A);
    }

    /**
     * Validates a plan for a query with aggregate and with sorting in descending order by a subset of grouping column.
     */
    @Test
    public void groupsWithOrderBySubsetOfGroupColumnDescending() throws Exception {
        checkDerivedCollationWithOrderBySubsetOfGroupColumnsSingle(TestCase.CASE_26);

        checkDerivedCollationWithOrderBySubsetOfGroupColumnsHash(TestCase.CASE_26A);
    }

    /**
     * Validates a plan for a query with GROUPING aggregate.
     */
    @Test
    public void groupsWithGroupingAggregate() throws Exception {
        checkSimpleAggWithGroupBySingle(TestCase.CASE_28_1A);
        assumeRun(TestCase.CASE_28_1B); // No collation can be utilized for the case if multiple group sets.

        checkSimpleAggWithGroupByHash(TestCase.CASE_28_2A);
        assumeRun(TestCase.CASE_28_2B); // No collation can be utilized for the case if multiple group sets.
    }

    private void checkSimpleAggSingle(TestCase testCase) throws Exception {
        checkSimpleAggSingle(testCase, hasAggregate());
    }

    private void checkSimpleAggSingle(TestCase testCase, Predicate<IgniteMapSortAggregate> aggPredicate) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(aggPredicate)
                                .and(hasGroups())
                                .and(input(isTableScan("TEST")))
                        ))
                ),
                disableRules
        );
    }

    private void checkSimpleAggHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(hasAggregate())
                                        .and(hasGroups())
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkSimpleAggWithGroupBySingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(hasGroups())
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkSimpleAggWithGroupByHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(hasAggregate())
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkDistinctAggSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(not(hasDistinctAggregate()))
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(not(hasAggregate()))
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkDistinctAggHash(TestCase testCase) throws Exception {
        assertPlan(testCase, nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(not(hasDistinctAggregate()))
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(hasDistribution(single()))
                                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                        .and(not(hasAggregate()))
                                                        .and(input(isInstanceOf(IgniteSort.class)
                                                                .and(input(isTableScan("TEST")))
                                                        ))
                                                ))
                                        ))
                                ))
                        ))),
                disableRules
        );
    }

    private void checkDistinctAggWithGroupBySingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(not(hasAggregate()))
                                                .and(hasGroups())
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))

                ),
                disableRules
        );
    }

    private void checkDistinctAggWithGroupByHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(hasDistribution(single()))
                                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                        .and(not(hasAggregate()))
                                                        .and(hasGroups())
                                                        .and(input(isInstanceOf(IgniteSort.class)
                                                                .and(input(isTableScan("TEST")))
                                                        ))
                                                ))
                                        ))
                                ))
                        ))

                ),
                disableRules
        );
    }

    private void checkAggWithGroupByIndexColumnsSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(input(isIndexScan("TEST", "idx_grp0_grp1")))
                        ))
                ),
                disableRules
        );
    }

    private void checkAggWithGroupByIndexColumnsHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(hasAggregate())
                                        .and(input(isIndexScan("TEST", "idx_grp0_grp1")))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkGroupWithNoAggregateSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkGroupWithNoAggregateHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkGroupWithNoAggregateUseIndexSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isIndexScan("TEST", "idx_grp0_grp1")))
                        ))
                ),
                disableRules
        );
    }

    private void checkGroupWithNoAggregateUseIndexHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isIndexScan("TEST", "idx_grp0_grp1")))
                                ))
                        ))
                ),
                disableRules
        );
    }

    private void checkGroupsWithOrderBySubsetOfGroupColumnsSingle(TestCase testCase, RelCollation collation) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> s.collation().equals(collation))
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))),
                disableRules
        );
    }

    private void checkGroupsWithOrderBySubsetOfGroupColumnsHash(TestCase testCase, RelCollation collation) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(hasDistribution(single()))
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(s -> s.collation().equals(collation))
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))),
                disableRules
        );
    }

    private void checkGroupsWithGroupBySubsetOfSortColumnsSingle(TestCase testCase, RelCollation collation) throws Exception {
        assertPlan(testCase,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1, 2))))
                        .and(input(isInstanceOf(IgniteProject.class)
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(s -> s.collation().equals(collation))
                                                        .and(input(isTableScan("TEST")
                                                        ))
                                                ))
                                        ))
                                ))
                        )),
                disableRules
        );
    }

    private void checkGroupsWithGroupBySubsetOfSortColumnsHash(TestCase testCase, RelCollation collation) throws Exception {
        assertPlan(testCase,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1, 2))))
                        .and(input(isInstanceOf(IgniteProject.class)
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(hasDistribution(single()))
                                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                        .and(input(isInstanceOf(IgniteSort.class)
                                                                .and(s -> s.collation().equals(collation))
                                                                .and(input(isTableScan("TEST")
                                                                ))
                                                        ))
                                                ))
                                        ))
                                ))
                        )),
                disableRules
        );
    }

    private void checkCountDistinctHash(TestCase testCase) throws Exception {
        Predicate<IgniteReduceSortAggregate> inputAgg = isInstanceOf(IgniteReduceSortAggregate.class)
                .and(hasGroupSets(IgniteReduceSortAggregate::getGroupSets, 0))
                .and(hasCollation(RelCollations.of(0)))
                .and(input(isInstanceOf(IgniteExchange.class)
                        .and(hasDistribution(single()))
                        .and(hasCollation(RelCollations.of(0)))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasGroupSets(Aggregate::getGroupSets, 1))
                        ))
                ));

        assertPlan(testCase, nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasNoGroupSets(IgniteReduceSortAggregate::getGroupSets))
                        .and(hasCollation(RelCollations.EMPTY))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasNoGroupSets(IgniteMapSortAggregate::getGroupSets))
                                .and(hasCollation(RelCollations.EMPTY))
                                .and(input(isInstanceOf(IgniteProject.class).and(input(inputAgg)))
                                ))
                        )),
                disableRules);
    }

    private void checkDerivedCollationWithOrderByGroupColumnSingle(TestCase testCase) throws Exception {
        RelCollation requiredCollation = RelCollations.of(TraitUtils.createFieldCollation(0, Collation.DESC_NULLS_FIRST));

        assertPlan(testCase, nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                .and(hasAggregate())
                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                        .and(hasAggregate())
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(hasCollation(requiredCollation))
                        ))
                ))

        ), disableRules);
    }

    private void checkDerivedCollationWithOrderByGroupColumnHash(TestCase testCase) throws Exception {
        RelCollation requiredCollation = RelCollations.of(TraitUtils.createFieldCollation(0, Collation.DESC_NULLS_FIRST));

        assertPlan(testCase, nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                .and(hasAggregate())
                .and(input(isInstanceOf(IgniteExchange.class)
                        .and(hasDistribution(single()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(hasCollation(requiredCollation))
                                ))
                        ))
                ))
        ), disableRules);
    }

    private void checkDerivedCollationWithOrderBySubsetOfGroupColumnsSingle(TestCase testCase) throws Exception {
        RelCollation requiredCollation = RelCollations.of(
                TraitUtils.createFieldCollation(1, Collation.DESC_NULLS_FIRST),
                TraitUtils.createFieldCollation(0, Collation.ASC_NULLS_LAST)
        );
        RelCollation outputCollation = RelCollations.of(
                TraitUtils.createFieldCollation(1, Collation.DESC_NULLS_FIRST)
        );

        assertPlan(testCase, isInstanceOf(Project.class)
                        .and(hasCollation(outputCollation))
                        .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(hasChildThat(hasCollation(requiredCollation)))
                                ))
                        )),
                disableRules);
    }

    private void checkDerivedCollationWithOrderBySubsetOfGroupColumnsHash(TestCase testCase) throws Exception {
        RelCollation requiredCollation = RelCollations.of(
                TraitUtils.createFieldCollation(1, Collation.DESC_NULLS_FIRST),
                TraitUtils.createFieldCollation(0, Collation.ASC_NULLS_LAST)
        );
        RelCollation outputCollation = RelCollations.of(
                TraitUtils.createFieldCollation(1, Collation.DESC_NULLS_FIRST)
        );

        assertPlan(testCase,
                isInstanceOf(IgniteProject.class)
                        .and(hasCollation(outputCollation))
                        .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(hasDistribution(single()))
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(hasCollation(requiredCollation))
                                                ))
                                        ))
                                ))
                        )),
                disableRules);
    }
}
