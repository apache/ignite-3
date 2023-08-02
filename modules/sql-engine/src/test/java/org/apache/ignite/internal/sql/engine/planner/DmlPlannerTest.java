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

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestTable;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify DML plans.
 */
public class DmlPlannerTest extends AbstractPlannerTest {

    /**
     * Test for INSERT .. VALUES when table has a single distribution.
     */
    @Test
    public void testInsertIntoSingleDistributedTable() throws Exception {
        IgniteTable test1 = newTestTable("TEST1", IgniteDistributions.single());
        IgniteSchema schema = createSchema(test1);

        // There should be no exchanges and other operations.
        assertPlan("INSERT INTO TEST1 (C0, C1, C2, C3) VALUES(0, 1, 2, 3)", schema,
                isInstanceOf(IgniteTableModify.class).and(input(isInstanceOf(IgniteValues.class))));
    }

    /**
     * Test for INSERT .. VALUES when table has non single distribution.
     */
    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testInsert(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);

        IgniteSchema schema = createSchema(test1);

        assertPlan("INSERT INTO TEST1 (C1, C2) VALUES(1, 2)", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteExchange.class).and(e -> distribution.equals(e.distribution())))))
        );
    }

    private static Stream<IgniteDistribution> nonSingleDistributions() {
        return distributions().filter(d -> !IgniteDistributions.single().equals(d));
    }

    /**
     * Test for INSERT .. FROM SELECT when tables has different distributions.
     */
    @ParameterizedTest
    @MethodSource("distributions")
    public void testInsertSelectFrom(IgniteDistribution distribution) throws Exception {
        IgniteDistribution anotherDistribution = IgniteDistributions.affinity(1, 1, "0");

        IgniteTable test1 = newTestTable("TEST1", distribution);
        IgniteTable test2 = newTestTable("TEST2", anotherDistribution);

        IgniteSchema schema = createSchema(test1, test2);

        assertPlan("INSERT INTO TEST1 (C1, C2) SELECT C1, C2 FROM TEST2", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteExchange.class).and(e -> distribution.equals(e.distribution())))))
        );
    }

    /**
     * Test for INSERT .. FROM SELECT when tables has the same distribution.
     */
    @ParameterizedTest
    @MethodSource("distributions")
    public void testInsertSelectFromSameDistribution(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);
        IgniteTable test2 = newTestTable("TEST2", distribution);

        IgniteSchema schema = createSchema(test1, test2);

        // there should be no exchanges.
        assertPlan("INSERT INTO TEST1 (C1, C2) SELECT C1, C2 FROM TEST2", schema,
                nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                        .and(hasChildThat(isInstanceOf(IgniteTableScan.class)))
        );
    }

    /**
     * Test for UPDATE when table has a single distribution.
     */
    @Test
    public void testUpdateOfSingleDistributedTable() throws Exception {
        IgniteTable test1 = newTestTable("TEST1", IgniteDistributions.single());
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
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class))
                                .and(hasChildThat(isInstanceOf(IgniteTableScan.class))))
        );
    }

    @ParameterizedTest
    @MethodSource("nonSingleDistributions")
    public void testDelete(IgniteDistribution distribution) throws Exception {
        IgniteTable test1 = newTestTable("TEST1", distribution);

        IgniteSchema schema = createSchema(test1);

        assertPlan("DELETE FROM TEST1 WHERE C0 = 1", schema,
                nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(e -> e.distribution().equals(IgniteDistributions.single())))
                        .and(nodeOrAnyChild(isInstanceOf(IgniteTableModify.class)
                                .and(input(isTableScan("TEST1")))))
        );
    }

    private static Stream<IgniteDistribution> distributions() {
        return Stream.of(
                IgniteDistributions.single(),
                IgniteDistributions.hash(List.of(0, 1)),
                IgniteDistributions.affinity(0, 2, "0"),
                IgniteDistributions.affinity(List.of(0, 2), 2, "0"),
                IgniteDistributions.affinity(List.of(1, 3), 2, "0")
        );
    }

    /**
     * Test for check basic dml operators when table doesn't exist.
     */
    @ParameterizedTest
    @MethodSource("basicStatements")
    public void testDmlQueriesOnNonExistingTable(String query) {
        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrowsWithCause(
                () -> physicalPlan(query, createSchema(newTestTable("TEST", IgniteDistributions.single()))),
                SqlValidatorException.class,
                "Object 'UNKNOWN' not found"
        );
    }

    private static Stream<String> basicStatements() {
        return Stream.of(
                "SELECT * FROM unknown",
                "INSERT INTO unknown VALUES(1)",
                "UPDATE unknown SET ID=1",
                "DELETE FROM unknown",
                "MERGE INTO unknown DST USING test SRC ON DST.C1 = SRC.C1"
                        + " WHEN MATCHED THEN UPDATE SET C2 = SRC.C2"
                        + " WHEN NOT MATCHED THEN INSERT (C1, C2) VALUES (SRC.C1, SRC.C2)",
                "MERGE INTO test DST USING unknown SRC ON DST.C1 = SRC.C1"
                        + " WHEN MATCHED THEN UPDATE SET C2 = SRC.C2"
                        + " WHEN NOT MATCHED THEN INSERT (C1, C2) VALUES (SRC.C1, SRC.C2)"
        );
    }

    // Class name is fully-qualified because AbstractPlannerTest defines a class with the same name.
    private static TestTable newTestTable(String tableName, IgniteDistribution distribution) {
        return TestBuilders.table()
                .name(tableName)
                .addKeyColumn("C0", NativeTypes.INT32)
                .addColumn("C1", NativeTypes.INT32)
                .addKeyColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32)
                .distribution(distribution)
                .build();
    }
}
