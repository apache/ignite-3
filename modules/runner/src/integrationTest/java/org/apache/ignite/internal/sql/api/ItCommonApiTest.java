/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionManager;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.junit.jupiter.api.Test;

/** Test common SQL API. */
public class ItCommonApiTest extends AbstractBasicIntegrationTest {
    protected SqlQueryProcessor queryProcessor() {
        return (SqlQueryProcessor) ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER_NODES.get(0).sql();
    }

    @Override
    protected int nodes() {
        return 1;
    }

    /** Check correctness of session expiration. */
    @Test
    public void testSessionExpiration() throws Exception {
        long timeout = TimeUnit.SECONDS.toMillis(10); // time from SessionManager.checkPeriod

        IgniteSql sql = igniteSql();

        var queryProc = queryProcessor();

        SessionManager sessionManager = IgniteTestUtils.getFieldValue(queryProc, "sessionManager");

        Map<SessionId, Session> activeSessions =
                IgniteTestUtils.getFieldValue(sessionManager, "activeSessions");

        Session ses1 = sql.sessionBuilder().defaultSessionTimeout(1, TimeUnit.SECONDS).build();
        Session ses2 = sql.sessionBuilder().defaultSessionTimeout(1000, TimeUnit.SECONDS).build();
        try {
            ses1.execute(null, "SELECT 1 + 1");
            ses2.execute(null, "SELECT 2 + 2");
        } catch (Throwable ignore) {
            fail();
        }

        //sessions is alive
        assertEquals(2, activeSessions.size());

        waitForCondition(() -> activeSessions.size() == 1, timeout + 2000);

        // the first session has been expired
        assertEquals(1, activeSessions.size());
        assertThrows(IgniteException.class, () -> ses1.execute(null, "SELECT 1 + 1"));
    }
}
