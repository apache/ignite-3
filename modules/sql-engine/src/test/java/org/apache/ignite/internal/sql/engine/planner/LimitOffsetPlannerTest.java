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

import static org.apache.ignite.internal.sql.engine.util.RexUtils.doubleFromRex;

import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Planner test for LIMIT and OFFSET.
 */
public class LimitOffsetPlannerTest extends AbstractPlannerTest {
    /** Rows count in table. */
    private static final int ROW_CNT = 100;

    @Test
    public void testLimit() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.broadcast());

        String sql = "SELECT * FROM TEST OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteLimit.class))
                .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));

        sql = "SELECT * FROM TEST ORDER BY ID OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                .and(hasChildThat(isInstanceOf(IgniteSort.class)))));
    }

    @Test
    public void testEstimateRows() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.random());

        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10", publicSchema,
                nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                    .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == 10d)));

        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT " + ROW_CNT + " OFFSET 15", publicSchema,
                nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                    .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == ROW_CNT - 15d)));

        assertPlan("SELECT * FROM TEST ORDER BY ID OFFSET 60", publicSchema,
                nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                    .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == ROW_CNT - 60d)));

        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 1 OFFSET " + ROW_CNT * 2, publicSchema,
                nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                        // Estimated row count returned by IgniteLimit node is 0, but after validation it becomes 1
                        // if it less than 1.
                        .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == 1)));
    }

    @Test
    public void testOrderOfRels() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.random());

        // Simple case, Limit can't be pushed down under Exchange or Sort. Sort before Exchange is more preferable.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 5 OFFSET 10", publicSchema,
                isInstanceOf(IgniteLimit.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> doubleFromRex(s.fetch, -1) == 5.0)
                                        .and(s -> doubleFromRex(s.offset, -1) == 10.0))))));

        // Same simple case but witout offset.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 5", publicSchema,
                isInstanceOf(IgniteLimit.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> doubleFromRex(s.fetch, -1) == 5.0)
                                        .and(s -> s.offset == null))))));

        // No special liited sort required if LIMIT is not set.
        assertPlan("SELECT * FROM TEST ORDER BY ID OFFSET 10", publicSchema,
                isInstanceOf(IgniteLimit.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> s.fetch == null)
                                        .and(s -> s.offset == null))))));

        // Simple case without ordering.
        assertPlan("SELECT * FROM TEST OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY", publicSchema,
                isInstanceOf(IgniteLimit.class)
                        .and(s -> doubleFromRex(s.fetch(), -1) == 5)
                        .and(s -> doubleFromRex(s.offset(), -1) == 10)
                        .and(input(isInstanceOf(IgniteExchange.class)))
                        .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));
        // Check that Sort node is not eliminated by aggregation and Exchange node is not eliminated by distribution
        // required by parent nodes.
        assertPlan("SELECT * FROM TEST UNION (SELECT * FROM TEST ORDER BY ID LIMIT 10)", publicSchema,
                nodeOrAnyChild(isInstanceOf(IgniteUnionAll.class)
                        .and(hasChildThat(isInstanceOf(IgniteLimit.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteSort.class)
                                                .and(s -> doubleFromRex(s.fetch, -1) == 10.0)))))))));

        // Check that internal Sort node is not eliminated by external Sort node with different collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID LIMIT 10) ORDER BY VAL", publicSchema,
                nodeOrAnyChild(isInstanceOf(IgniteSort.class)
                    .and(hasChildThat(isInstanceOf(IgniteLimit.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> doubleFromRex(s.fetch, -1) == 10.0)))))))));

        // Check that extended collation is passed through the Limit node if it satisfies the Limit collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID LIMIT 10) ORDER BY ID, VAL", publicSchema,
                isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(input(isInstanceOf(IgniteTableScan.class)))
                                .and(s -> s.collation().getKeys().equals(ImmutableIntList.of(0, 1))))))));

        // Check that external Sort node is not required if external collation is subset of internal collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID, VAL LIMIT 10) ORDER BY ID", publicSchema,
                isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                            .and(input(isInstanceOf(IgniteSort.class)
                                    .and(s -> doubleFromRex(s.fetch, -1) == 10.0))))));

        // Check double limit when external collation is a subset of internal collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID, VAL LIMIT 10) ORDER BY ID LIMIT 5 OFFSET 3",
                publicSchema,
                isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteLimit.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> doubleFromRex(s.fetch, -1) == 10.0)
                                        .and(s -> s.offset == null))))))));

        // Check limit/exchange/sort rel order in subquery.
        assertPlan("SELECT NULLIF((SELECT id FROM test ORDER BY id LIMIT 1 OFFSET 1), id) FROM test",
                publicSchema,
                hasChildThat(isInstanceOf(IgniteLimit.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(e -> e.distribution() == IgniteDistributions.single())
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> doubleFromRex(s.offset, -1) == 1)
                                        .and(s -> doubleFromRex(s.fetch, -1) == 1)))))));

        publicSchema = createSchemaWithTable(IgniteDistributions.random(), "ID");

        // Sort node is not required, since collation of the Limit node equals to the index collation.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10 OFFSET 10", publicSchema,
                isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)))))
                    .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));

        publicSchema = createSchemaWithTable(IgniteDistributions.random(), "ID", "VAL");

        // Sort node is not required, since collation of the Limit node satisfies the index collation.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10 OFFSET 10", publicSchema,
                isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)))))
                    .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));

        publicSchema = createSchemaWithTable(IgniteDistributions.single());

        // Exchange node is not required, since distribution of the table is already "single".
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10 OFFSET 10", publicSchema,
                isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteSort.class)))
                    .and(hasChildThat(isInstanceOf(IgniteExchange.class)).negate()));
    }

    @Test
    public void testNestedOffset() throws Exception {
        TableBuilder builder = TestBuilders.table()
                .name("TEST")
                .addColumn("A", NativeTypes.INT32)
                .size(ROW_CNT)
                .distribution(IgniteDistributions.random());

        IgniteSchema publicSchema = createSchema(builder.build());

        assertPlan("SELECT a FROM (SELECT a FROM test ORDER BY a OFFSET 2)", publicSchema,
                isInstanceOf(IgniteLimit.class)
                        .and(s -> doubleFromRex(s.offset(), -1) == 2.0)
                        .and(input(isInstanceOf(IgniteExchange.class))
                                .and(hasChildThat(isInstanceOf(IgniteSort.class)
                                ))));

        assertPlan("SELECT a FROM (SELECT a FROM test ORDER BY a OFFSET 2) t(a) UNION ALL SELECT a FROM test",
                publicSchema, isInstanceOf(IgniteUnionAll.class)
                        .and(hasChildThat(isInstanceOf(IgniteLimit.class)
                                .and(s -> doubleFromRex(s.offset(), -1) == 2.0)
                                .and(input(isInstanceOf(IgniteExchange.class))
                                        .and(hasChildThat(isInstanceOf(IgniteSort.class)
                                                .and(s -> s.offset == null)))))
                        )
                        .and(hasChildThat(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteTableScan.class))))
                        ));

        assertPlan("SELECT a FROM (SELECT a FROM test ORDER BY a OFFSET 2) t(a) UNION ALL SELECT a FROM test ORDER BY a",
                publicSchema, isInstanceOf(IgniteSort.class)
                        .and(s -> s.offset == null && s.fetch == null)
                        .and(hasChildThat(isInstanceOf(IgniteUnionAll.class)
                                .and(hasChildThat(isInstanceOf(IgniteLimit.class)
                                        .and(s -> doubleFromRex(s.offset(), -1) == 2.0)
                                        .and(input(isInstanceOf(IgniteExchange.class))
                                                .and(hasChildThat(isInstanceOf(IgniteSort.class)
                                                        .and(s -> s.offset == null)))))
                                )
                                .and(hasChildThat(isInstanceOf(IgniteExchange.class)
                                        .and(input(isInstanceOf(IgniteTableScan.class))))
                                )
                        ))
        );
    }

    /**
     * Creates PUBLIC schema with one TEST table.
     */
    private static IgniteSchema createSchemaWithTable(IgniteDistribution distr, String... indexesColumns) {
        TableBuilder builder = TestBuilders.table()
                .name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .size(ROW_CNT)
                .distribution(distr);

        if (indexesColumns.length > 0) {
            addSortIndex(indexesColumns).apply(builder);
        }

        return createSchema(builder.build());
    }
}
