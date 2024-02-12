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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.util.IgniteNameUtils.quote;
import static org.apache.ignite.table.criteria.Criteria.and;
import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Set;
import java.util.function.UnaryOperator;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * SQL generation with index hint test.
 */
class SqlSerializerIndexHintTest extends AbstractPlannerTest {
    private static IgniteSchema SCHEMA;

    private static final String TBL1 = "TBL1";

    @BeforeAll
    public static void setup() {
        SCHEMA = AbstractPlannerTest.createSchemaFrom(
                createSimpleTable(TBL1, 100)
                        .andThen(AbstractPlannerTest.addHashIndex("ID"))
                        .andThen(AbstractPlannerTest.addSortIndex("NAME"))
        );
    }

    @Test
    void testForceIndexHint() throws Exception {
        SqlSerializer ser = new SqlSerializer.Builder()
                .tableName(TBL1)
                .indexName("idx_name")
                .columns(Set.of("ID", "NAME"))
                .where(and(columnValue("id", equalTo(1)), columnValue("name", equalTo("v"))))
                .build();

        String sql = ser.toString();

        assertThat(sql, startsWith(format("SELECT /*+ FORCE_INDEX({}) */ * FROM", quote("IDX_NAME"))));
        assertArrayEquals(new Object[]{1, "v"}, ser.getArguments());

        assertPlan(sql, SCHEMA, nodeOrAnyChild(isIndexScan(TBL1, "IDX_NAME")));
    }

    private static UnaryOperator<TableBuilder> createSimpleTable(String name, int sz) {
        return t -> t.name(name)
                .size(sz)
                .distribution(IgniteDistributions.single())
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("NAME", NativeTypes.STRING);
    }
}
