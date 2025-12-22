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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests to verify SQL query execution during concurrent schema updates.
 */
public class ItOutdatedPlanTest extends BaseSqlIntegrationTest {
    private int numberOfFinishedTransactionsOnStart;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void setup() {
        numberOfFinishedTransactionsOnStart = txManager().finished();
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "ignite.sql.execution.threadCount: 64";
    }

    @Test
    void testConcurrentDdlWritesMultistep() {
        IgniteSql sql = CLUSTER.aliveNode().sql();

        sql("CREATE TABLE t(id INT PRIMARY KEY)");

        int iterations = 10;

        for (int i = 0; i < iterations; i++) {
            log.info("iteration #" + i);

            CompletableFuture<AsyncResultSet<SqlRow>> ddlFut = sql.executeAsync(
                    null, IgniteStringFormatter.format("ALTER TABLE t ADD COLUMN val{} VARCHAR DEFAULT 'abc{}'", i, i));

            CompletableFuture<AsyncResultSet<SqlRow>> fut = sql.executeAsync(
                    null, "INSERT INTO t (ID) SELECT x * 100 / 2 FROM SYSTEM_RANGE(0, 10)");

            await(await(fut).closeAsync());
            await(await(ddlFut).closeAsync());

            sql("DELETE FROM t");

            verifyFinishedTxCount(2 * (i + 1));
        }
    }

    // same as above fast plan
    // same InternalSchemaVersionMismatchException
    @Test
    @Disabled
    void testConcurrentDdlWritesFastPlan() {
        IgniteSql sql = CLUSTER.aliveNode().sql();

        sql("create table t(id int primary key)");

        sql.executeAsync(null, "alter table t add column val varchar default 'abc'");

        CompletableFuture<AsyncResultSet<SqlRow>> fut = sql.executeAsync(
                null, "insert into t (id) values (0)");

        // Fails
        // org.apache.ignite.internal.partition.replicator.schemacompat.InternalSchemaVersionMismatchException
        // at ... SchemaCompatibilityValidator.failIfRequestSchemaDiffersFromTxTs(SchemaCompatibilityValidator.java:291)
        // at ... TableAwareReplicaRequestPreProcessor.lambda$preProcessTableAwareRequest$0(TableAwareReplicaRequestPreProcessor.java:112)
        await(fut);

        System.out.println(sql("select * from t"));
    }

    // Fails with IncompatibleSchemaVersionException
    // This raises if tx observes not last schema
    // see failIfSchemaChangedAfterTxStart
    // Operation must be restarted with new transaction
    @Test
    @Disabled
    void testConcurrentDdlReadsAndInsert() throws InterruptedException {
        IgniteSql sql = CLUSTER.aliveNode().sql();

        for (int i = 0; i < 100; i++) {
            System.out.println(">xxx> #" + i);
            sql("create table t(id int primary key)");
            sql("create table v(id int primary key)");
            sql("insert into t (id) SELECT x * 100 / 2 FROM system_range(0, 10)");
            sql.executeAsync(null, "alter table t add column val varchar default 'abc'");

            int interval = ThreadLocalRandom.current().nextInt(5) * 25;
            Thread.sleep(interval);

            // Failing here
            System.out.println(sql("insert into v select t2.id * 3 + t1.id from t as t1 join t as t2 on t1.id=t2.id"));

            sql("drop table t");
            sql("drop table v");
        }
    }

    private void verifyFinishedTxCount(int expected) {
        int expectedTotal = numberOfFinishedTransactionsOnStart + expected;

        boolean success;

        try {
            success = waitForCondition(() -> expectedTotal == txManager().finished(), 2_000);

            if (!success) {
                assertEquals(expectedTotal, txManager().finished());
            }

            assertEquals(0, txManager().pending());
        } catch (InterruptedException e) {
            fail("Thread has been interrupted", e);
        }
    }
}
