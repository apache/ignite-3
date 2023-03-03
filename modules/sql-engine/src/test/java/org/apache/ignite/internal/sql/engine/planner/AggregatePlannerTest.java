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

import java.util.function.Predicate;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.junit.jupiter.api.Test;

/**
 * Test that verifies plans for queries with aggregates.
 *
 * <p>Note: The test should not disable any rules and validates the best query plans.
 */
public class AggregatePlannerTest extends AbstractAggregatePlannerTest {
    /** {@inheritDoc} */
    @Override
    protected String[] disabledRules() {
        return new String[0];
    }

    @Test
    public void case1() throws Exception {
        assertPlan(TestCase.CASE_1, any -> true);
        assertPlan(TestCase.CASE_2, any -> true);
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase1(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasAggregate())
                        .and(input(isTableScan("TEST")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase2(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(hasAggregate())
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase3(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasAggregate())
                        .and(input(isTableScan("TEST")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase4(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
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

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase5(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(hasGroups())
                                .and(input(isTableScan("TEST")))
                        ))
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase6(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(hasGroups())
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase7(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasDistinctAggregate())
                        .and(hasGroups())
                        .and(input(isTableScan("TEST")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase8(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasDistinctAggregate())
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase9(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(hasAggregate())
                        .and(hasChildThat(isIndexScan("TEST", "grp0_grp1")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase10(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(hasAggregate())
                                                .and(input(isIndexScan("TEST", "grp0_grp1")))
                                        ))
                                )
                        ))
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase11(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(hasAggregate())
                        .and(hasChildThat(isIndexScan("TEST", "grp0_grp1")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase12(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(hasAggregate())
                                                .and(input(isIndexScan("TEST", "grp0_grp1")))
                                        ))
                                )
                        ))
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase13(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isTableScan("TEST")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase14(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
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

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase15(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isIndexScan("TEST", "grp0_grp1")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase16(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isIndexScan("TEST", "grp0_grp1")))
                                ))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase17(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasAggregate())
                        .and(input(isTableScan("TEST")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase18(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(hasAggregate())
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase19(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isTableScan("TEST")))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase20(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteSort.class)
                        .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(not(hasAggregate()))
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase21(String sql, IgniteSchema schema, String... additionalRules) throws Exception {
        // Ignore given additionalRules to check what is the best plan.
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(input(isIndexScan("TEST", "val0")))
                ));
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase22(String sql, IgniteSchema schema, String... additionalRules) throws Exception {
        // Ignore given additionalRules to check what is the best plan.
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isIndexScan("TEST", "val0")))
                                ))
                        ))
                ));
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase23(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(input(isInstanceOf(IgniteLimit.class)
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                ));
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase24(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(input(isInstanceOf(IgniteLimit.class)
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))
                ));
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase25(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(s -> s.collation().equals(collation))
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase26(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> s.collation().equals(collation))
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase27(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(s -> s.collation().equals(collation))
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase28(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> s.collation().equals(collation))
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase29(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(collation))
                        .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(input(isTableScan("TEST")))
                        ))
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase30(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(collation))
                        .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase31(String sql, IgniteSchema schema) throws Exception {
        Predicate<? extends RelNode> subtreePredicate = isInstanceOf(IgniteColocatedSortAggregate.class)
                // Check the second aggregation step contains accumulators.
                // Plan must not contain distinct accumulators.
                .and(hasAggregate())
                .and(not(hasDistinctAggregate()))
                // Check the first aggregation step is SELECT DISTINCT (doesn't contain any accumulators)
                .and(input(isInstanceOf(IgniteColocatedSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                ));

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ));
    }

    /** {@inheritDoc} */
    @Override
    protected void checkTestCase32(String sql, IgniteSchema schema) throws Exception {
        Predicate<? extends RelNode> subtreePredicate = isInstanceOf(IgniteColocatedSortAggregate.class)
                // Check the second aggregation step contains accumulators.
                // Plan must not contain distinct accumulators.
                .and(hasAggregate())
                .and(not(hasDistinctAggregate()))
                // Check the first aggregation step is SELECT DISTINCT (doesn't contain any accumulators)
                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                ))
                        ))
                ));

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ));
    }
}
