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

import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify DML plans.
 */
public class DmlPlannerTest extends AbstractPlannerTest {
    /**
     * Test for INSERT .. FROM SELECT when a both tables has a single distribution.
     * TODO: IGNITE-19018 split into 2 cases: colocated and non-colocated.
     */
    @Test
    public void testInsertFromSelectSingleDistribution() throws Exception {
        IgniteTable test1 = newTestTable("TEST1", single());
        IgniteTable test2 = newTestTable("TEST2", single());

        IgniteSchema schema = createSchema(test1, test2);

        String query = "INSERT INTO TEST1 (C1, C2) SELECT * FROM TEST2";

        // There should be no exchanges and other operations.
        assertPlan(query, schema,
                isInstanceOf(IgniteTableModify.class)
                        .and(e -> e.distribution().equals(single()))
                        .and(input(isInstanceOf(IgniteTableScan.class)))
        );
    }

    /**
     * Test for INSERT .. FROM SELECT when a target table has a single distribution.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testInsertIntoSingleDistributedTableFromSelect(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", single());
        IgniteTable test2 = newTestTable("TEST2", distribution);

        IgniteSchema schema = createSchema(test1, test2);

        String query = "INSERT INTO TEST1 (C1, C2) SELECT * FROM TEST2";

        assertPlan(query, schema,
                isInstanceOf(IgniteTableModify.class)
                        .and(e -> e.distribution().equals(single()))
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(e -> e.distribution().equals(single()))
                                .and(input(isInstanceOf(IgniteTableScan.class)))
                        ))
        );
    }

    /**
     * Test for INSERT .. FROM SELECT when a source table has a single distribution.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testInsertFromSingleDistributedTable(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);
        IgniteTable test2 = newTestTable("TEST2", single());

        IgniteSchema schema = createSchema(test1, test2);

        String query = "INSERT INTO TEST1 (C1, C2) SELECT * FROM TEST2";

        assertPlan(query, schema,
                isInstanceOf(IgniteProject.class)
                        .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(e -> single().equals(e.distribution()))
                                        .and(input(isInstanceOf(IgniteTableModify.class)
                                                .and(e -> e.distribution().equals(distribution))
                                                .and(input(isInstanceOf(IgniteExchange.class)
                                                        .and(input(isInstanceOf(IgniteTableScan.class)))
                                                ))
                                        ))
                                ))
                        ))
        );
    }

    /**
     * Test for INSERT .. VALUES.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testInsertValues(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);

        IgniteSchema schema = createSchema(test1);

        assertPlan("INSERT INTO TEST1 (C1, C2) VALUES(1, 2)", schema,
                hasChildThat(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteExchange.class).and(e -> distribution.equals(e.distribution())))))
        );
    }

    private static Stream<IgniteDistribution> nonSingleDistributions() {
        return distributions().filter(d -> !single().equals(d));
    }

    /**
     * Test for INSERT .. FROM SELECT when tables has different distributions.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testInsertSelectFromNonColocated(IgniteDistribution distribution) throws Exception {
        IgniteDistribution anotherDistribution = IgniteDistributions.affinity(1, new UUID(1, 0), "0");

        IgniteTable test1 = newTestTable("TEST1", distribution);
        IgniteTable test2 = newTestTable("TEST2", anotherDistribution);

        IgniteSchema schema = createSchema(test1, test2);

        assertPlan("INSERT INTO TEST1 (C1, C2) SELECT C1, C2 FROM TEST2", schema,
                hasChildThat(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(single()))
                        .and(input(isInstanceOf(IgniteTableModify.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(e -> distribution.equals(e.distribution()))
                                        .and(input(isInstanceOf(IgniteTableScan.class)))
                                ))
                        ))
                ));
    }

    /**
     * Test for INSERT .. FROM SELECT when tables has the same distribution.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testInsertSelectFromSameDistribution(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);
        IgniteTable test2 = newTestTable("TEST2", distribution);

        IgniteSchema schema = createSchema(test1, test2);

        // there should be no exchanges.
        assertPlan("INSERT INTO TEST1 (C1, C2) SELECT C1, C2 FROM TEST2", schema,
                isInstanceOf(IgniteProject.class)
                        .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                                .and(input(isInstanceOf(IgniteExchange.class)
                                        .and(e -> single().equals(e.distribution()))
                                        .and(input(isInstanceOf(IgniteTableModify.class)
                                                .and(e -> e.distribution().equals(distribution))
                                                .and(input(isInstanceOf(IgniteTableScan.class)))
                                        ))
                                ))
                        )));
    }

    /**
     * Test for UPDATE when table has a single distribution.
     */
    @Test
    public void testUpdateOfSingleDistributedTable() throws Exception {
        IgniteTable test1 = newTestTable("TEST1", single());
        IgniteSchema schema = createSchema(test1);

        // There should be no exchanges and other operations.
        assertPlan("UPDATE TEST1 SET C2 = C2 + 1", schema,
                isInstanceOf(IgniteTableModify.class).and(input(isInstanceOf(IgniteTableScan.class))));
    }

    /**
     * Test for UPDATE when table has non single distribution.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testUpdate(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);

        IgniteSchema schema = createSchema(test1);

        assertPlan("UPDATE TEST1 SET C2 = C2 + 1", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteTableScan.class))))
        );
    }

    private static Stream<IgniteDistribution> distributions() {
        return Stream.of(
                single(),
                IgniteDistributions.hash(List.of(0, 1)),
                IgniteDistributions.affinity(0, new UUID(1, 1), "0")
        );
    }

    // Class name is fully-qualified because AbstractPlannerTest defines a class with the same name.
    private static org.apache.ignite.internal.sql.engine.framework.TestTable newTestTable(
            String tableName, IgniteDistribution distribution) {

        return TestBuilders.table()
                .name(tableName)
                .addColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .distribution(distribution)
                .build();
    }
}
