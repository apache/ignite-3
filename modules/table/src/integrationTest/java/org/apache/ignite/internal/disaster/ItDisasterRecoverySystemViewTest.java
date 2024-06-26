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

package org.apache.ignite.internal.disaster;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.AVAILABLE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** For integration testing of disaster recovery system views. */
public class ItDisasterRecoverySystemViewTest extends BaseSqlIntegrationTest {
    private static final String ZONE_NAME = "ZONE_" + TABLE_NAME;

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    @BeforeAll
    protected void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        assertThat(systemViewManager().completeRegistration(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);
    }

    @Test
    void testNoZonesAndTables() {
        assertQuery(globalPartitionStatesSystemViewSql()).returnNothing().check();
        assertQuery(localPartitionStatesSystemViewSql()).returnNothing().check();
    }

    @Test
    void testGlobalPartitionStatesSystemView() {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), 2);

        assertQuery(globalPartitionStatesSystemViewSql())
                .returns(ZONE_NAME, TABLE_NAME, 0, AVAILABLE.name())
                .returns(ZONE_NAME, TABLE_NAME, 1, AVAILABLE.name())
                .check();
    }

    @Test
    void testLocalPartitionStatesSystemView() {
        assertEquals(2, initialNodes());

        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), 2);

        List<String> nodeNames = CLUSTER.runningNodes().map(IgniteImpl::name).sorted().collect(toList());

        String nodeName0 = nodeNames.get(0);
        String nodeName1 = nodeNames.get(1);

        assertQuery(localPartitionStatesSystemViewSql())
                .returns(nodeName0, ZONE_NAME, TABLE_NAME, 0, HEALTHY.name())
                .returns(nodeName0, ZONE_NAME, TABLE_NAME, 1, HEALTHY.name())
                .returns(nodeName1, ZONE_NAME, TABLE_NAME, 0, HEALTHY.name())
                .returns(nodeName1, ZONE_NAME, TABLE_NAME, 1, HEALTHY.name())
                .check();
    }

    private static String globalPartitionStatesSystemViewSql() {
        return "SELECT ZONE_NAME, TABLE_NAME, PARTITION_ID, STATE FROM SYSTEM.GLOBAL_PARTITION_STATES";
    }

    private static String localPartitionStatesSystemViewSql() {
        return "SELECT NODE_NAME, ZONE_NAME, TABLE_NAME, PARTITION_ID, STATE FROM SYSTEM.LOCAL_PARTITION_STATES";
    }
}
