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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests to verify SQL query execution during concurrent schema updates.
 */
public class ItOutdatedPlanTest extends BaseSqlIntegrationTest {
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
        dropAllTables();
    }

    @Test
    void multiStepDmlWithConcurrentDdl() throws InterruptedException {
        IgniteSql sql = CLUSTER.aliveNode().sql();

        sql("CREATE TABLE t(id INT PRIMARY KEY)");

        int iterations = 10;

        for (int i = 0; i < iterations; i++) {
            log.info("iteration #" + i);

            String ddlQuery = i % 2 == 0
                    ? "ALTER TABLE t ADD COLUMN val VARCHAR DEFAULT 'abc'"
                    : "ALTER TABLE t DROP COLUMN val";

            CompletableFuture<AsyncResultSet<SqlRow>> ddlFut = sql.executeAsync(null, ddlQuery);

            Thread.sleep(i * ThreadLocalRandom.current().nextInt(10));

            sql.execute(null, "INSERT INTO t (ID) SELECT x * 100 / 2 FROM SYSTEM_RANGE(0, 10)").close();

            await(await(ddlFut).closeAsync());

            assertEquals(0, txManager().pending());

            sql("DELETE FROM t");
        }
    }

    @Test
    void fastDmlWithConcurrentDdl() throws InterruptedException {
        IgniteSql sql = CLUSTER.aliveNode().sql();

        sql("CREATE TABLE t(id INT PRIMARY KEY)");

        int iterations = 20;

        for (int i = 0; i < iterations; i++) {
            log.info("iteration #" + i);

            String ddlQuery = i % 2 == 0
                    ? "ALTER TABLE t ADD COLUMN val VARCHAR DEFAULT 'abc'"
                    : "ALTER TABLE t DROP COLUMN val";

            CompletableFuture<AsyncResultSet<SqlRow>> ddlFut = sql.executeAsync(null, ddlQuery);
            Thread.sleep(ThreadLocalRandom.current().nextInt(15) * 10);

            sql.execute(null, "INSERT INTO t (id) VALUES ?", i).close();

            await(await(ddlFut).closeAsync());

            assertEquals(0, txManager().pending());
        }
    }
}
