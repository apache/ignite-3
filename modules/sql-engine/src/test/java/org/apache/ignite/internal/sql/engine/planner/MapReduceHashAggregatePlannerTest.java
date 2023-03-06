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

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test to verify MapReduceHashAggregateConverterRule usage by a planner.
 */
public class MapReduceHashAggregatePlannerTest extends AbstractAggregatePlannerTest {
    private final String[] disableRules = {
            "MapReduceSortAggregateConverterRule",
            "ColocatedHashAggregateConverterRule",
            "ColocatedSortAggregateConverterRule"
    };

    /** {@inheritDoc} */
    @Override
    protected String[] disabledRules() {
        return disableRules;
    }

    @SuppressWarnings("MethodOverridesStaticMethodOfSuperclass")
    @BeforeAll
    static void initMissedCases() {
        AbstractAggregatePlannerTest.initMissedCases();

        AbstractAggregatePlannerTest.missedCases.removeAll(List.of(
                //TODO: https://issues.apache.org/jira/browse/IGNITE-18871 Wrong collation derived.
                TestCase.CASE_18_3, TestCase.CASE_18_3A
        ));
    }

    /**
     * Validates a plan for simple query with aggregate.
     */
    @Test
    protected void simpleAggregate() throws Exception {
        checkSimpleAggSingle(TestCase.CASE_1);
        checkSimpleAggHash(TestCase.CASE_1A);
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
        checkDistinctAggSingle(TestCase.CASE_2_3);

        checkDistinctAggHash(TestCase.CASE_2_1A);
        checkDistinctAggHash(TestCase.CASE_2_2A);
        checkDistinctAggHash(TestCase.CASE_2_3A);
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
        checkDistinctAggWithGroupBySingle(TestCase.CASE_7_4);

        checkDistinctAggWithGroupByHash(TestCase.CASE_7_1A);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_2A);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_3A);
        checkDistinctAggWithGroupByHash(TestCase.CASE_7_4A);
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
    }

    /**
     * Validates a plan for a query with DISTINCT and without aggregation function.
     */
    @Test
    public void distinctWithoutAggregate() throws Exception {
        checkGroupWithNoAggregateSingle(TestCase.CASE_12);
        checkGroupWithNoAggregateHash(TestCase.CASE_12A);
    }

    /**
     * Validates a plan uses index for a query with DISTINCT and without aggregation functions.
     */
    @Test
    public void distinctWithoutAggregateUseIndex() throws Exception {
        checkGroupWithNoAggregateSingle(TestCase.CASE_13);
        checkGroupWithNoAggregateHash(TestCase.CASE_13A);
    }

    /**
     * Validates a plan for a query which WHERE clause contains a sub-query with aggregate.
     */
    @Test
    public void subqueryWithAggregateInWhereClause() throws Exception {
        checkSimpleAggSingle(TestCase.CASE_14);
        checkSimpleAggHash(TestCase.CASE_14A);
    }

    /**
     * Validates a plan for a query with DISTINCT aggregate in WHERE clause.
     */
    @Test
    public void distinctAggregateInWhereClause() throws Exception {
        checkGroupWithNoAggregateSingle(TestCase.CASE_15);
        checkGroupWithNoAggregateHash(TestCase.CASE_15A);
    }

    /**
     * Validates a plan with merge-sort utilizes index if collation fits.
     */
    @Test
    public void noSortAppendingWithCorrectCollation() throws Exception {
        String[] additionalRulesToDisable = {"NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "CorrelateToNestedLoopRule"};

        assertPlan(TestCase.CASE_16,
                nodeOrAnyChild(isInstanceOf(IgniteSort.class)
                        .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                ),
                additionalRulesToDisable);

        assertPlan(TestCase.CASE_16A,
                nodeOrAnyChild(isInstanceOf(IgniteSort.class)
                        .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                ),
                additionalRulesToDisable);
    }

    /**
     * Validates a plan for a sub-query with order and limit.
     */
    @Test
    public void emptyCollationPassThroughLimit() throws Exception {
        assertPlan(TestCase.CASE_17,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(input(isInstanceOf(IgniteLimit.class)
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))
                ));

        assertPlan(TestCase.CASE_17A,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(input(isInstanceOf(IgniteLimit.class)
                                                .and(input(isInstanceOf(IgniteExchange.class)
                                                        .and(input(isInstanceOf(IgniteSort.class)
                                                                .and(input(isTableScan("TEST")))
                                                        ))
                                                ))
                                        ))
                                ))
                        ))
                ));
    }

    /**
     * Validates a plan for a query with aggregate and with groups and sorting by the same column set.
     */
    @Test
    public void groupsWithOrderByGroupColumns() throws Exception {
        checkGroupsWithOrderByGroupColumnsSingle(TestCase.CASE_18_1, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderByGroupColumnsSingle(TestCase.CASE_18_2, TraitUtils.createCollation(List.of(1, 0)));
        // checkGroupsWithOrderByGroupColumnsSingle(TestCase.CASE_18_3, TraitUtils.createCollation(List.of(0, 1)));

        checkGroupsWithOrderByGroupColumnsHash(TestCase.CASE_18_1A, TraitUtils.createCollation(List.of(0, 1)));
        checkGroupsWithOrderByGroupColumnsHash(TestCase.CASE_18_2A, TraitUtils.createCollation(List.of(1, 0)));
        // checkGroupsWithOrderByGroupColumnsHash(TestCase.CASE_18_3A, TraitUtils.createCollation(List.of(0, 1)));
    }

    /**
     * Validates a plan for a query with aggregate and with sorting by subset of group columns.
     */
    @Test
    public void aggregateWithOrderByGroupColumns() throws Exception {
        checkGroupsWithOrderByGroupColumnsSingle(TestCase.CASE_19_1, TraitUtils.createCollation(List.of(0)));
        checkGroupsWithOrderByGroupColumnsSingle(TestCase.CASE_19_2, TraitUtils.createCollation(List.of(1)));
        checkGroupsWithOrderByGroupColumnsSingle(TestCase.CASE_20, TraitUtils.createCollation(List.of(0, 1, 2)));

        checkGroupsWithOrderByGroupColumnsHash(TestCase.CASE_19_1A, TraitUtils.createCollation(List.of(0)));
        checkGroupsWithOrderByGroupColumnsHash(TestCase.CASE_19_2A, TraitUtils.createCollation(List.of(1)));
        checkGroupsWithOrderByGroupColumnsHash(TestCase.CASE_20A, TraitUtils.createCollation(List.of(0, 1, 2)));
    }

    /**
     * Validates a plan for a query with aggregate and groups, and EXPAND_DISTINCT_AGG hint.
     */
    @Test
    public void expandDistinctAggregates() throws Exception {
        Predicate<? extends RelNode> subtreePredicate = nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                // Check the second aggregation step contains accumulators.
                // Plan must not contain distinct accumulators.
                .and(hasAggregate())
                .and(not(hasDistinctAggregate()))
                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                        // Check the first aggregation step is SELECT DISTINCT (doesn't contain any accumulators)
                        .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                ))
                        ))
                ))
        );

        assertPlan(TestCase.CASE_21, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ));

        subtreePredicate = nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                // Check the second aggregation step contains accumulators.
                // Plan must not contain distinct accumulators.
                .and(hasAggregate())
                .and(not(hasDistinctAggregate()))
                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                        // Check the first aggregation step is SELECT DISTINCT (doesn't contain any accumulators)
                        .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                                .and(not(hasAggregate()))
                                                .and(hasGroups())
                                        ))
                                ))
                        ))
                ))
        );

        assertPlan(TestCase.CASE_21A, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ));
    }

    private void checkSimpleAggSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(hasAggregate())
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

    private void checkSimpleAggHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(hasAggregate())
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    private void checkSimpleAggWithGroupBySingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(hasAggregate())
                                .and(hasGroups())
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

    private void checkSimpleAggWithGroupByHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(hasAggregate())
                                        .and(hasGroups())
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    private void checkDistinctAggSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(hasAggregate())
                                .and(not(hasDistinctAggregate()))
                                .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                                .and(not(hasAggregate()))
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                )
        );
    }

    private void checkDistinctAggHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(hasAggregate())
                                .and(not(hasDistinctAggregate()))
                                .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                                        .and(not(hasAggregate()))
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))
        );
    }

    private void checkDistinctAggWithGroupBySingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                                .and(not(hasAggregate()))
                                                .and(hasGroups())
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))

                )
        );
    }

    private void checkDistinctAggWithGroupByHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                                        .and(not(hasAggregate()))
                                                        .and(hasGroups())
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))

                )
        );
    }

    //TODO: https://issues.apache.org/jira/browse/IGNITE-18871 Index should be used here.
    private void checkAggWithGroupByIndexColumnsSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(hasAggregate())
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

    //TODO: https://issues.apache.org/jira/browse/IGNITE-18871 Index should be used here.
    private void checkAggWithGroupByIndexColumnsHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(hasAggregate())
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    private void checkGroupWithNoAggregateSingle(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

    private void checkGroupWithNoAggregateHash(TestCase testCase) throws Exception {
        assertPlan(testCase,
                nodeOrAnyChild(isInstanceOf(IgniteReduceHashAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }


    private void checkGroupsWithOrderByGroupColumnsSingle(TestCase testCase, RelCollation collation) throws Exception {
        assertPlan(testCase,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(collation))
                        .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
        );
    }

    private void checkGroupsWithOrderByGroupColumnsHash(TestCase testCase, RelCollation collation) throws Exception {
        assertPlan(testCase,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(collation))
                        .and(input(isInstanceOf(IgniteReduceHashAggregate.class)
                                .and(input(isInstanceOf(IgniteMapHashAggregate.class)
                                        //TODO: Why can't Map be pushed down to under 'exchange'.
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
        );
    }
}
