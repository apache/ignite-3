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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration tests to verify SQL query execution during concurrent schema updates.
 */
public class ItSqlConcurrentSchemaModificationTest extends BaseSqlIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "ignite.sql.execution.threadCount: 64";
    }

    @AfterEach
    void dropTables() {
        System.clearProperty("FAST_QUERY_OPTIMIZATION_ENABLED");
        Commons.resetFastQueryOptimizationFlag();
        unwrapIgniteImpl(CLUSTER.aliveNode()).queryEngine().invalidatePlannerCache(Set.of());

        dropAllTables();
    }

    @ParameterizedTest(name = "FastQueryOptimization={0}")
    @ValueSource(booleans = {true, false})
    void dmlWithConcurrentDdl(Boolean fastPlan) throws InterruptedException {
        System.setProperty("FAST_QUERY_OPTIMIZATION_ENABLED", fastPlan.toString());

        IgniteSql sql = CLUSTER.aliveNode().sql();

        sql("CREATE TABLE t(id INT PRIMARY KEY)");

        int iterations = 20;

        for (int i = 0; i < iterations; i++) {
            log.info("iteration #" + i);

            String ddlQuery = i % 2 == 0
                    ? "ALTER TABLE t ADD COLUMN val VARCHAR DEFAULT 'abc'"
                    : "ALTER TABLE t DROP COLUMN val";

            CompletableFuture<AsyncResultSet<SqlRow>> ddlFut = sql.executeAsync(ddlQuery);

            Thread.sleep(ThreadLocalRandom.current().nextInt(15) * 10);

            assertQuery("INSERT INTO t (id) VALUES (?)")
                    .withParam(i)
                    .returns(1L)
                    .check();

            await(await(ddlFut).closeAsync());

            assertEquals(0, txManager().pending());
        }
    }

    @ParameterizedTest(name = "FastQueryOptimization={0}")
    @ValueSource(booleans = {true, false})
    void selectWithConcurrentDdl(boolean fastPlan) throws InterruptedException {
        System.setProperty("FAST_QUERY_OPTIMIZATION_ENABLED", String.valueOf(fastPlan));

        IgniteSql sql = CLUSTER.aliveNode().sql();

        sql("CREATE TABLE t(id INT PRIMARY KEY, id2 INT)");
        sql("INSERT INTO t VALUES (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)");

        int iterations = 20;

        for (int i = 0; i < iterations; i++) {
            log.info("iteration #" + i);

            String ddlQuery = i % 2 == 0
                    ? "ALTER TABLE t ADD COLUMN val VARCHAR DEFAULT 'abc'"
                    : "ALTER TABLE t DROP COLUMN val";

            CompletableFuture<AsyncResultSet<SqlRow>> ddlFut = sql.executeAsync(ddlQuery);
            Thread.sleep(ThreadLocalRandom.current().nextInt(15) * 10);

            int id = i % 10;

            assertQuery(format("SELECT id2 FROM t WHERE id={}", id))
                    .returns(id)
                    .check();

            await(await(ddlFut).closeAsync());

            assertEquals(0, txManager().pending());
        }
    }
}
