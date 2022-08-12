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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.Map;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.session.SessionManager;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.junit.jupiter.api.Test;

/** Test common SQL API. */
public class ItCommonApiTest extends AbstractBasicIntegrationTest {
    protected QueryProcessor queryProcessor() {
        return ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER_NODES.get(0).sql();
    }

    @Override protected int nodes() {
        return 1;
    }

    /** Check correctness of session expiration. */
    @Test
    public void testSessionExpiration() throws Exception {
        int timeout = 100;
        IgniteSql sql = igniteSql();
        Session ses1 = sql.sessionBuilder().build();

        SqlQueryProcessor qProc = (SqlQueryProcessor) queryProcessor();

        SessionManager sessionManager = IgniteTestUtils.getFieldValue(qProc, "sessionManager");

        Cache<Object, org.apache.ignite.internal.sql.engine.session.Session> activeSessions =
                IgniteTestUtils.getFieldValue(sessionManager, "activeSessions");

        for (Map.Entry<Object, org.apache.ignite.internal.sql.engine.session.Session> ent : activeSessions.asMap().entrySet()) {
            org.apache.ignite.internal.sql.engine.session.Session ses0 = ent.getValue();

            IgniteTestUtils.setFieldValue(ses0, org.apache.ignite.internal.sql.engine.session.Session.class,
                    "idleTimeoutMs", timeout);
        }

        try {
            ses1.execute(null, "SELECT 1 + 1");
        } catch (Throwable ignore) {
            // No op.
        }

        Thread.sleep(2 * timeout);

        activeSessions.cleanUp();

        Session ses2 = sql.sessionBuilder().build();

        assertThrows(IgniteException.class, () -> ses1.execute(null, "SELECT 1 + 1").close());

        ses2.execute(null, "SELECT 1 + 1").close();
    }
}
