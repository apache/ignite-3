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
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * Test to verify MapReduceHashAggregateConverterRule usage by a planner.
 */
public class MapReduceSortAggregatePlannerTest extends AbstractAggregatePlannerTest {
    private final String[] disableRules = {
            "MapReduceHashAggregateConverterRule",
            "ColocatedHashAggregateConverterRule",
            "ColocatedSortAggregateConverterRule"
    };

    @Override
    protected String[] disabledRules() {
        return disableRules;
    }

    @Override
    protected void checkTestCase1(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(input(isTableScan("TEST")))
                        ))
                )
        );
    }

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

    @Override
    protected void checkTestCase3(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    @Override
    protected void checkTestCase4(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(hasAggregate())
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                )
                        ))
        );
    }

    @Override
    protected void checkTestCase5(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(input(isIndexScan("TEST", "grp0_grp1")))
                        ))
                )
        );
    }

    @Override
    protected void checkTestCase6(String sql, IgniteSchema schema) throws Exception {
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

    @Override
    protected void checkTestCase7(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasAggregate())
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(not(hasDistinctAggregate()))
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(hasGroups())
                                                .and(not(hasAggregate()))
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))
                )
        );
    }

    @Override
    protected void checkTestCase8(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasAggregate())
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(hasAggregate())
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                        .and(hasGroups())
                                                        .and(not(hasAggregate()))
                                                        .and(input(isInstanceOf(IgniteSort.class)
                                                                .and(input(isTableScan("TEST")))
                                                        ))
                                                ))
                                        ))
                                ))
                        ))
        );
    }


    @Override
    protected void checkTestCase9(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
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
                )
        );
    }

    @Override
    protected void checkTestCase10(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                )
        );
    }

    @Override
    protected void checkTestCase11(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(not(hasAggregate()))
                                .and(hasGroups())
                                .and(input(isIndexScan("TEST", "grp0_grp1")))
                        ))
                )
        );
    }

    @Override
    protected void checkTestCase12(String sql, IgniteSchema schema) throws Exception {
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


    @Override
    protected void checkTestCase13(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
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

                )
        );
    }

    @Override
    protected void checkTestCase14(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(hasAggregate())
                        .and(not(hasDistinctAggregate()))
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteExchange.class)
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

                )
        );
    }

    @Override
    protected void checkTestCase15(String sql, IgniteSchema schema, String... additionalRules) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(input(isIndexScan("TEST", "val0")))
                        ))
                ),
                ArrayUtils.concat(disabledRules(), additionalRules));
    }

    @Override
    protected void checkTestCase16(String sql, IgniteSchema schema, String... additionalRules) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isIndexScan("TEST", "val0")))
                                ))
                        ))
                ),
                ArrayUtils.concat(disabledRules(), additionalRules));
    }

    @Override
    protected void checkTestCase17(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> s.collation().equals(collation))
                                        .and(input(isTableScan("TEST")))
                                ))
                        ))
                )
        );
    }

    @Override
    protected void checkTestCase18(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(input(isInstanceOf(IgniteMapSortAggregate.class) //TODO: Why can't Map be pushed down to under 'exchange'.
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(s -> s.collation().equals(collation))
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                )
        );
    }

    @Override
    protected void checkTestCase19(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(collation))
                        .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
        );
    }

    @Override
    protected void checkTestCase20(String sql, IgniteSchema schema, RelCollation collation) throws Exception {
        assertPlan(sql, schema,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(collation))
                        .and(input(isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        //TODO: Why can't Map be pushed down to under 'exchange'.
                                        .and(input(isInstanceOf(IgniteExchange.class)
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))
        );
    }

    @Override
    protected void checkTestCase21(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(input(isInstanceOf(IgniteLimit.class)
                                                .and(input(isInstanceOf(IgniteSort.class)
                                                        .and(input(isTableScan("TEST")))
                                                ))
                                        ))
                                ))
                        ))
                ));
    }

    @Override
    protected void checkTestCase22(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteReduceSortAggregate.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
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

    @Override
    protected void checkTestCase23(String sql, IgniteSchema schema) throws Exception {
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

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ));
    }

    @Override
    protected void checkTestCase24(String sql, IgniteSchema schema) throws Exception {
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
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                                .and(not(hasAggregate()))
                                                .and(hasGroups())
                                        ))
                                ))
                        ))
                ));

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, subtreePredicate))
                .and(input(1, subtreePredicate))
        ));
    }

    @Override
    protected void checkTestCase25(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema,
                nodeOrAnyChild(isInstanceOf(IgniteReduceSortAggregate.class)
                        .and(not(hasAggregate()))
                        .and(hasGroups())
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteMapSortAggregate.class)
                                        .and(not(hasAggregate()))
                                        .and(hasGroups())
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(input(isTableScan("TEST")))
                                        ))
                                ))
                        ))
                )
        );
    }
}
