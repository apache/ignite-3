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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.junit.jupiter.api.Test;

/**
 * Checks the processing of the observable timestamp passed from the client side.
 */
public class ItSqlObservableTimestampTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int nodes() {
        return 1;
    }

    @Test
    public void timestampGrowsUp() throws InterruptedException {
        String query = "select 1";

        HybridTimestamp minTimestamp = HybridTimestamp.MIN_VALUE;

        HybridTimestamp newTimestamp = execAndGetTimestamp(query, minTimestamp);

        assertThat(newTimestamp, greaterThan(minTimestamp));

        waitForCondition(() -> !execAndGetTimestamp(query, newTimestamp).equals(newTimestamp), 10_000);

        assertThat(execAndGetTimestamp(query, newTimestamp), greaterThan(newTimestamp));
    }

    @Test
    public void timestampNotReturnedForRwOperation() {
        sql("CREATE TABLE test(id INT PRIMARY KEY)");

        HybridTimestamp ts = execAndGetTimestamp("INSERT INTO test VALUES(1)", new HybridClockImpl().now());

        assertThat(ts, nullValue());
    }

    private HybridTimestamp execAndGetTimestamp(String query, HybridTimestamp inputTs) {
        SessionId sessionId = queryProcessor().createSession(PropertiesHelper.emptyHolder());
        QueryContext ctx = QueryContext.create(SqlQueryType.ALL, inputTs);

        AsyncSqlCursor<?> cursor = await(
                queryProcessor().querySingleAsync(sessionId, ctx, query)
        );

        return cursor.implicitTxReadTimestamp();
    }
}
