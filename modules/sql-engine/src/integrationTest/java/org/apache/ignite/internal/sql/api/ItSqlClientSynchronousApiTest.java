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

package org.apache.ignite.internal.sql.api;

import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.tx.IgniteTransactions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Tests for synchronous client SQL API.
 */
public class ItSqlClientSynchronousApiTest extends ItSqlSynchronousApiTest {
    private IgniteClient client;

    @BeforeAll
    public void startClient() {
        client = IgniteClient.builder().addresses(getClientAddresses(List.of(CLUSTER.aliveNode())).get(0)).build();
    }

    @AfterAll
    public void stopClient() throws Exception {
        client.close();
    }

    @Override
    protected IgniteSql igniteSql() {
        return client.sql();
    }

    @Override
    protected IgniteTransactions igniteTx() {
        return client.transactions();
    }
}
