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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelled;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelledInternalException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Set of test cases for multi-statement query cancellation. */
public class ItCancelScriptTest extends BaseSqlMultiStatementTest {
    @BeforeAll
    void createTestTable() {
        sql("CREATE TABLE test (id INT PRIMARY KEY)");

        waitUntilRunningQueriesCount(is(0));
    }

    @Override
    protected int initialNodes() {
        return 2;
    }

    @AfterEach
    void cleanup() {
        sql("DELETE FROM TEST");

        waitUntilRunningQueriesCount(is(0));
    }

    @Test
    public void cancelScript() {
        StringBuilder query = new StringBuilder();

        int statementsCount = 100;

        for (int j = 0; j < statementsCount; j++) {
            query.append("SELECT ").append(j).append(";");
        }

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        List<AsyncSqlCursor<InternalSqlRow>> allCursors = fetchAllCursors(runScript(token, query.toString()));

        assertThat(allCursors, hasSize(statementsCount));
        assertThat(queryProcessor().runningQueries().size(), is(statementsCount + /* SCRIPT */ 1));

        cancelHandle.cancel();

        allCursors.forEach(cursor -> expectQueryCancelled(
                new DrainCursor(cursor)
        ));
    }

    @Test
    void cancelMustRollbackScriptTransaction() throws InterruptedException {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        AsyncSqlCursor<InternalSqlRow> cur = runScript(token,
                "START TRANSACTION READ WRITE;"
                + "INSERT INTO test VALUES(0);"
                + "INSERT INTO test VALUES(1);"
                + "COMMIT;"
        );

        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchCursors(cur, 3, false);
        assertThat(cursors, hasSize(3));

        assertEquals(1, txManager().pending());

        assertThat(queryProcessor().openedCursors(), is(2));

        cancelHandle.cancel();

        assertTrue(waitForCondition(() -> queryProcessor().openedCursors() == 0, 5_000));
        waitUntilRunningQueriesCount(is(0));
        assertEquals(0, txManager().pending());

        assertTrue(cursors.get(2).hasNextResult());

        expectQueryCancelledInternalException(
                () -> await(cursors.get(2).nextResult())
        );

        // Ensure that the transaction was rolled back.
        assertQuery("SELECT count(*) FROM test")
                .returns(0L)
                .check();
    }

    @Test
    public void cancelScriptBeforeExecution() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancel();

        expectQueryCancelledInternalException(
                () -> runScript(token, "SELECT 1; SELECT 2;")
        );
    }
}
