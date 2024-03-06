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

package org.apache.ignite.internal.index;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.Test;

/** Integration test to check the work with indexes on rebalancing. */
public class ItIndexAndRebalanceTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = zoneName(TABLE_NAME);

    private static final String INDEX_NAME = "TEST_INDEX";

    private static final String COLUMN_NAME = "SALARY";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void testChangeReplicaCountWithoutRestartNodes() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 2, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        changeZoneReplicas(ZONE_NAME, 1);
        insertPeople(TABLE_NAME, new Person(1, "1", 11.0));

        changeZoneReplicas(ZONE_NAME, 2);
        insertPeople(TABLE_NAME, new Person(2, "2", 12.0));

        List<IgniteImpl> nodes = CLUSTER.runningNodes().collect(toList());

        assertThat(nodes, hasSize(2));

        for (IgniteImpl node : nodes) {
            assertQuery(node, format("SELECT * FROM {} WHERE {} > 0.0", TABLE_NAME, COLUMN_NAME))
                    .matches(containsIndexScan(DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                    .returnRowCount(3)
                    .check();
        }
    }

    private static void changeZoneReplicas(String zoneName, int replicas) throws Exception {
        sql(format("ALTER ZONE {} SET REPLICAS={}", zoneName, replicas));

        // TODO: IGNITE-21501 нужен другой способ подождать в тесте завершения ребеланса
        Thread.sleep(3_333);
    }
}
