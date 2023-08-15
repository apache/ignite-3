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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.api.AsyncResultSetEx;
import org.apache.ignite.internal.sql.api.SessionEx;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Blah-blah-blah.
 */
public class ItSqlObservableTimestampTest extends ClusterPerClassIntegrationTest {

    private static SessionEx session;

    @Override
    protected int nodes() {
        return 1;
    }

    @BeforeAll
    static void beforeAll() {
        sql("CREATE TABLE test (id int primary key, val int)");

        session = (SessionEx) sql().createSession();
    }

    private static IgniteSql sql() {
        return CLUSTER_NODES.get(0).sql();
    }

    @Test
    public void outTimestampGrowsUp() {
        HybridTimestamp minTimestamp = HybridTimestamp.MIN_VALUE;

        assertThat(execAndGetTimestamp(minTimestamp), greaterThan(minTimestamp));
    }

    @Test
    public void outTimestampNotChangesDuringSafeTime() {
        HybridTimestamp nowTimestamp = new HybridClockImpl().now();

        assertThat(execAndGetTimestamp(nowTimestamp), equalTo(nowTimestamp));
    }

    private HybridTimestamp execAndGetTimestamp(HybridTimestamp inputTs) {
        AsyncResultSetEx<SqlRow> resultSet = await(
                session.executeAsyncInternal(inputTs, sql().createStatement("select 1"))
        );

        return resultSet.implicitTxReadTimestamp();
    }
}
