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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.ZoneId;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code SQL_CACHED_QUERY_PLANS} view.
 */
public class ItCachedQueryPlansSystemViewTest extends AbstractSystemViewTest {

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    public void testMetadata() {
        assertQuery("SELECT * FROM SYSTEM.SQL_CACHED_QUERY_PLANS")
                .columnMetadata(
                        new MetadataMatcher().name("NODE_ID").type(ColumnType.STRING).nullable(false),
                        new MetadataMatcher().name("PLAN_ID").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("CATALOG_VERSION").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("QUERY_DEFAULT_SCHEMA").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("SQL").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("QUERY_TYPE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("QUERY_PLAN").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("QUERY_PREPARE_TIME").type(ColumnType.TIMESTAMP)
                                .precision(NativeTypes.MAX_TIME_PRECISION).nullable(true)
                )
                .check();
    }

    @Test
    public void test() throws InterruptedException {
        CatalogManager catalogManager = TestWrappers.unwrapIgniteImpl(node(0)).catalogManager();

        int v1 = catalogManager.latestCatalogVersion();

        String plan1 = executeAndGetPlan(0, "PUBLIC", "SELECT 1");
        String plan2 = executeAndGetPlan(1, "PUBLIC", "SELECT 2");

        // increase catalog version
        sql("CREATE SCHEMA test_schema");
        sql("CREATE TABLE test_table (id INT, val INT, PRIMARY KEY(id))");

        int v2 = catalogManager.latestCatalogVersion();
        assertNotEquals(v1, v2, "catalog versions should differ");

        String plan3 = executeAndGetPlan(0, "TEST_SCHEMA", "SELECT 3");
        String plan4 = executeAndGetPlan(1, "PUBLIC", "INSERT INTO test_table (id, val) SELECT x, y FROM (VALUES(1, 2)) t(x, y)");
        String plan5 = executeAndGetPlan(0, "PUBLIC", "SELECT * FROM test_table WHERE id = 1");

        String node1 = node(0).name();
        String node2 = node(1).name();

        String selectCachedQueries = "SELECT "
                + "node_id, catalog_version, query_default_schema, sql, query_type, query_plan "
                + "FROM system.sql_cached_query_plans "
                // exclude this query out the result.
                + "WHERE sql not like '%SQL_CACHED_QUERY_PLANS%' "
                + "ORDER BY sql";

        assertQuery(selectCachedQueries)
                .returns(node2, v2, "PUBLIC", "INSERT INTO `TEST_TABLE` (`ID`, `VAL`)\n"
                        + "SELECT `X`, `Y`\n"
                        + "FROM (VALUES ROW(1, 2)) AS `T` (`X`, `Y`)", "DML", plan4)
                .returns(node1, v2, "PUBLIC", "SELECT *\n"
                        + "FROM `TEST_TABLE`\n"
                        + "WHERE `ID` = 1", "Query", plan5)
                .returns(node1, v1, "PUBLIC", "SELECT 1", "Query", plan1)
                .returns(node2, v1, "PUBLIC", "SELECT 2", "Query", plan2)
                .returns(node1, v2, "TEST_SCHEMA", "SELECT 3", "Query", plan3)
                .ordered()
                .check();
    }

    private static String executeAndGetPlan(int nodeIndex, String schema, String query) {
        return sql(node(nodeIndex), (Transaction) null, schema, ZoneId.systemDefault(),
                "EXPLAIN PLAN FOR " + query)
                .get(0).get(0).toString();
    }
}
