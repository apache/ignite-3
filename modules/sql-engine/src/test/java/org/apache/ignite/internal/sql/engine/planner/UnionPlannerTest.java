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

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.Union;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @Test
    public void testUnionAllResultsInLeastRestrictiveType() throws Exception {
        IgniteSchema publicSchema = createSchema(
                TestBuilders.table()
                        .name("TABLE1")
                        .addColumn("C1", NativeTypes.INT32)
                        .addColumn("C2", NativeTypes.STRING)
                        .distribution(someAffinity())
                        .build(),

                TestBuilders.table()
                        .name("TABLE2")
                        .addColumn("C1", NativeTypes.DOUBLE)
                        .addColumn("C2", NativeTypes.STRING)
                        .distribution(someAffinity())
                        .build(),

                TestBuilders.table()
                        .name("TABLE3")
                        .addColumn("C1", NativeTypes.INT64)
                        .addColumn("C2", NativeTypes.STRING)
                        .distribution(someAffinity())
                        .build()
        );

        String sql = ""
                + "SELECT * FROM table1 "
                + "UNION ALL "
                + "SELECT * FROM table2 "
                + "UNION ALL "
                + "SELECT * FROM table3 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteUnionAll.class)
                .and(input(0, tableWithProjection("TABLE1", "CAST($t0):DOUBLE", "$t1"))
                .and(input(1, hasChildThat(isTableScan("TABLE2"))))
                .and(input(2, tableWithProjection("TABLE3", "CAST($t0):DOUBLE", "$t1"))))
        );
    }

    @Test
    public void testUnionAllDifferentNullability() throws Exception {
        IgniteSchema publicSchema = createSchema(
                TestBuilders.table()
                        .name("TABLE1")
                        .addColumn("C1", NativeTypes.INT32, false)
                        .addColumn("C2", NativeTypes.STRING)
                        .distribution(someAffinity())
                        .build(),

                TestBuilders.table()
                        .name("TABLE2")
                        .addColumn("C1", NativeTypes.INT32, true)
                        .addColumn("C2", NativeTypes.STRING)
                        .distribution(someAffinity())
                        .build()
        );

        String sql = ""
                + "SELECT * FROM table1 "
                + "UNION ALL "
                + "SELECT * FROM table2";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteUnionAll.class)
                .and(input(0, isInstanceOf(IgniteExchange.class).and(input(isTableScan("TABLE1"))))
                        .and(input(1, isInstanceOf(IgniteExchange.class).and(input(isTableScan("TABLE2"))))))
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {50, 100})
    void unionAllHugeNumberOfTables(int tablesCount) throws Exception {
        IgniteTable[] tables = IntStream.range(0, tablesCount)
                .mapToObj(i -> createTestTable("TABLE_" + i))
                .toArray(IgniteTable[]::new);

        IgniteSchema schema = createSchema(tables);
        String query = IntStream.range(0, tablesCount)
                .mapToObj(i -> "SELECT \n"
                        + "  'label_" + i + "'  As l_name, \n"
                        + "  CASE WHEN EXISTS (\n"
                        + "    SELECT 1 FROM table_" + i + "\n"
                        + "  ) then true ELSE false END AS has_data")
                .collect(Collectors.joining(" UNION "));

        // No special verifications. We just expect this query to complete successfully. 
        assertPlan(query, schema, node -> true);
    }

    /**
     * Create schema for tests.
     *
     * @return Ignite schema.
     */
    private static IgniteSchema prepareSchema() {
        return createSchema(
                createTestTable("TABLE1"),
                createTestTable("TABLE2"),
                createTestTable("TABLE3")
        );
    }

    private static IgniteTable createTestTable(String tableName) {
        return TestBuilders.table()
                .name(tableName)
                .distribution(someAffinity())
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("NAME", NativeTypes.STRING)
                .addColumn("SALARY", NativeTypes.DOUBLE)
                .build();
    }
}
