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

import io.netty.util.ResourceLeakDetector;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.manager.IgniteTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

/**
 * Tests partition awareness.
 */
public class PartitionAwarenessTest extends AbstractClientTest {
    protected static TestServer testServer2;

    protected static IgniteClient client2;

    protected static int serverPort2;

    /**
     * Before all.
     */
    @BeforeAll
    public static void beforeAll() {
        AbstractClientTest.beforeAll();

        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        testServer2 = startServer(10800, 10, 0, server, "server-2");
        serverPort2 = testServer2.port();

        client2 = startClient("127.0.0.1:" + serverPort, "127.0.0.1:" + serverPort2);
    }

    /**
     * After all.
     */
    @AfterAll
    public static void afterAll() throws Exception {
        AbstractClientTest.afterAll();

        testServer2.close();
    }

    @Test
    public void testGetRoutesRequestToPrimaryNode() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        recordView.get(null, Tuple.create().set("id", 1L));
        recordView.get(null, Tuple.create().set("id", 2L));
    }

    protected Table defaultTable() {
        FakeIgniteTables tables = (FakeIgniteTables) server.tables();
        TableImpl tableImpl = (TableImpl) tables.createTable(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        ArrayList<String> assignments = new ArrayList<>();
        assignments.add(testServer.nodeId());
        assignments.add(testServer2.nodeId());
        assignments.add(testServer.nodeId());
        assignments.add(testServer2.nodeId());

        tables.setPartitionAssignments(tableImpl.tableId(), assignments);

        return client2.tables().table(DEFAULT_TABLE);
    }
}
