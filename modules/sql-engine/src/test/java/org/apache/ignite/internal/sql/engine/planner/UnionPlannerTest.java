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
import org.apache.calcite.rel.core.Union;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Union test.
 */
public class UnionPlannerTest extends AbstractPlannerTest {
    @Test
    public void testUnion() throws Exception {
        IgniteSchema publicSchema = prepareSchema();

        String sql = ""
                + "SELECT * FROM table1 "
                + "UNION "
                + "SELECT * FROM table2 "
                + "UNION "
                + "SELECT * FROM table3 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
                .and(hasDistribution(IgniteDistributions.single()))
                .and(input(isInstanceOf(IgniteColocatedHashAggregate.class)
                        .and(hasChildThat(isInstanceOf(Union.class)
                                .and(input(0, isTableScan("TABLE1")))
                                .and(input(1, isTableScan("TABLE2")))
                                .and(input(2, isTableScan("TABLE3")))
                        ))
                ))
        );
    }

    @Test
    public void testUnionAll() throws Exception {
        IgniteSchema publicSchema = prepareSchema();

        String sql = ""
                + "SELECT * FROM table1 "
                + "UNION ALL "
                + "SELECT * FROM table2 "
                + "UNION ALL "
                + "SELECT * FROM table3 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteUnionAll.class)
                .and(input(0, hasChildThat(isTableScan("TABLE1"))))
                .and(input(1, hasChildThat(isTableScan("TABLE2"))))
                .and(input(2, hasChildThat(isTableScan("TABLE3")))));
    }

    /**
     * Create schema for tests.
     *
     * @return Ignite schema.
     */
    private static IgniteSchema prepareSchema() {
        return createSchemaFrom(
                createTestTable("TABLE1"),
                createTestTable("TABLE2"),
                createTestTable("TABLE3")
        );
    }

    private static UnaryOperator<TableBuilder> createTestTable(String tableName) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .distribution(someAffinity())
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("NAME", NativeTypes.STRING)
                .addColumn("SALARY", NativeTypes.DOUBLE);
    }
}
