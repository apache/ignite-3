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

import java.util.Set;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Checks the processing of the observable timestamp passed from the client side.
 */
public class ItSqlObservableTimestampTest extends ClusterPerClassIntegrationTest {

    private SessionId sessionId;

    @Override
    protected int nodes() {
        return 1;
    }

    @BeforeAll
    void beforeAll() {
        sessionId = queryProcessor().createSession(PropertiesHelper.emptyHolder());
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
        AsyncSqlCursor<?> cursor = await(
                queryProcessor().querySingleAsync(sessionId, QueryContext.create(Set.of(SqlQueryType.QUERY), inputTs), "select 1")
        );

        return cursor.implicitTxReadTimestamp();
    }
}
