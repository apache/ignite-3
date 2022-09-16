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

import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceAggregateBase.class)
                .and(hasChildThat(isInstanceOf(Union.class)
                )));
        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceAggregateBase.class)
                .and(hasChildThat(isInstanceOf(Union.class)
                        .and(input(0, isTableScan("TABLE1")))
                        .and(input(1, isTableScan("TABLE2")))
                        .and(input(2, isTableScan("TABLE3")))
                )));
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
    private IgniteSchema prepareSchema() {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl1 = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("SALARY", f.createJavaType(Double.class))
                        .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Table1", "hash");
            }
        };

        TestTable tbl2 = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("SALARY", f.createJavaType(Double.class))
                        .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Table2", "hash");
            }
        };

        TestTable tbl3 = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("SALARY", f.createJavaType(Double.class))
                        .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Table3", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TABLE1", tbl1);
        publicSchema.addTable("TABLE2", tbl2);
        publicSchema.addTable("TABLE3", tbl3);

        return publicSchema;
    }
}
