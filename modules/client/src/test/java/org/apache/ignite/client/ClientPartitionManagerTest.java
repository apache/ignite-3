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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for client partition manager.
 */
public class ClientPartitionManagerTest extends AbstractClientTest {
    private static final String TABLE_NAME = "tbl1";

    @BeforeEach
    public void setUp() {
        ((FakeIgniteTables) server.tables()).createTable(TABLE_NAME);
    }

    @Test
    public void primaryReplicas() {
        Table table = client.tables().table(TABLE_NAME);
        PartitionManager partMgr = table.partitionManager();

        Map<Partition, ClusterNode> map = partMgr.primaryReplicasAsync().join();
        assertEquals(4, map.size());
    }
}
