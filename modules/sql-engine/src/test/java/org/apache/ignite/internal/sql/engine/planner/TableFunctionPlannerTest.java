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

import java.util.function.UnaryOperator;
import org.apache.calcite.rel.core.Join;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test table functions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TableFunctionPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /**
     * Setup.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @BeforeAll
    public void setup() {
        publicSchema = createSchemaFrom(
                createTestTable("RANDOM_TBL", IgniteDistributions.random()),
                createTestTable("BROADCAST_TBL", IgniteDistributions.broadcast())
        );
    }

    /**
     * TestTableFunctionScan.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTableFunctionScan() throws Exception {
        String sql = "SELECT * FROM TABLE(system_range(1, 1))";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableFunctionScan.class));
    }

    /**
     * TestBroadcastTableAndTableFunctionJoin.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBroadcastTableAndTableFunctionJoin() throws Exception {
        String sql = "SELECT * FROM broadcast_tbl t JOIN TABLE(system_range(1, 1)) r ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(Join.class)
                        .and(input(0, nodeOrAnyChild(isTableScan("broadcast_tbl"))))
                        .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableFunctionScan.class))))
                )));
    }

    /**
     * TestRandomTableAndTableFunctionJoin.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRandomTableAndTableFunctionJoin() throws Exception {
        String sql = "SELECT * FROM random_tbl t JOIN TABLE(system_range(1, 1)) r ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(Join.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(nodeOrAnyChild(isTableScan("random_tbl"))))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableFunctionScan.class))))
        ));
    }

    /**
     * TestCorrelatedTableFunctionJoin.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorrelatedTableFunctionJoin() throws Exception {
        String sql = "SELECT t.id, (SELECT x FROM TABLE(system_range(t.id, t.id))) FROM random_tbl t";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                .and(input(0, nodeOrAnyChild(isTableScan("random_tbl"))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableFunctionScan.class))))
        ));
    }

    private static UnaryOperator<TableBuilder> createTestTable(String tableName, IgniteDistribution distribution) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .distribution(distribution)
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("NAME", NativeTypes.STRING)
                .addColumn("SALARY", NativeTypes.DOUBLE);
    }
}
