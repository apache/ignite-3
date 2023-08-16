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
import static org.hamcrest.Matchers.greaterThan;

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
    public void timestampGrowsUp() {
        HybridTimestamp minTimestamp = HybridTimestamp.MIN_VALUE;

        assertThat(execAndGetTimestamp(minTimestamp), greaterThan(minTimestamp));
    }

    private HybridTimestamp execAndGetTimestamp(HybridTimestamp inputTs) {
        SessionId sessionId = queryProcessor().createSession(PropertiesHelper.emptyHolder());
        QueryContext ctx = QueryContext.create(SqlQueryType.ALL, inputTs);

        AsyncSqlCursor<?> cursor = await(
                queryProcessor().querySingleAsync(sessionId, ctx, "select 1")
        );

        return cursor.implicitTxReadTimestamp();
    }
}
