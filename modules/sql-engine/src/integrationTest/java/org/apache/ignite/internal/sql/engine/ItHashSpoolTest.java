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

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Hash spool test.
 */
public class ItHashSpoolTest extends BaseSqlIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterPerClassIntegrationTest.class);

    /**
     * After each.
     */
    @AfterEach
    protected void cleanUp() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Start cleanUp()");
        }

        for (Table table : CLUSTER.aliveNode().tables().tables()) {
            sql("DROP TABLE " + table.name());
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("End cleanUp()");
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18689")
    @SuppressWarnings("unchecked")
    public void testHashSpoolCondition() {
        sql("CREATE TABLE t(id INT PRIMARY KEY, i INTEGER)");
        sql("INSERT INTO t VALUES (0, 0), (1, 1), (2, 2)");

        String sql = "SELECT i, (SELECT i FROM t WHERE i=t1.i AND i-1=0) FROM t AS t1";

        assertQuery(sql)
                .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
                .returns(0, null)
                .returns(1, 1)
                .returns(2, null)
                .check();
    }
}
