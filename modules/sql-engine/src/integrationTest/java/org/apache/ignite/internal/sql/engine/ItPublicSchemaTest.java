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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * SQL tests for public schema.
 */
public class ItPublicSchemaTest extends ClusterPerTestIntegrationTest {

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    public void existsInEmptyCluster() {
        CatalogManager catalogManager = TestWrappers.unwrapIgniteImpl(cluster.node(0)).catalogManager();

        assertNotNull(catalogManager.latestCatalog().schema("PUBLIC"));
    }

    @Test
    public void dropCreatePublicSchema() {
        sql("PUBLIC", "CREATE SCHEMA s2");
        sql("S2", "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR)");

        sql("PUBLIC", "DROP SCHEMA PUBLIC");

        // Both should fail as PUBLIC schema has been dropped.
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Object 'T1' not found",
                () -> sql("PUBLIC", "SELECT count(*) FROM t1")
        );
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Object 'PUBLIC' not found",
                () -> sql("PUBLIC", "SELECT count(*) FROM PUBLIC.t1")
        );

        // Non default schema
        sql("S2", "SELECT count(*) FROM t1");
        // Fully qualified table names also work.
        sql("S2", "SELECT count(*) FROM S2.t1");

        // Recreate public schema
        sql("PUBLIC", "CREATE SCHEMA public");
        sql("PUBLIC", "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR)");

        sql("PUBLIC", "SELECT count(*) FROM t1");
        sql("S2", "SELECT count(*) FROM public.t1");
        sql("MISSING", "SELECT count(*) FROM public.t1");
    }

    private void sql(String defaultSchema, String stmt) {
        IgniteSql sql = cluster.node(0).sql();
        Statement statement = sql.statementBuilder().query(stmt)
                .defaultSchema(defaultSchema)
                .build();

        try (ResultSet<?> rs = sql.execute((Transaction) null, statement)) {
            assertTrue(rs.hasRowSet() || rs.wasApplied());
        }
    }
}
