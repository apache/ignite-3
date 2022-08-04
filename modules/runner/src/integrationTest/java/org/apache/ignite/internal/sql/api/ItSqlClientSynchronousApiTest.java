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

import static org.apache.ignite.internal.runner.app.client.ItAbstractThinClientTest.getClientAddresses;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for synchronous client SQL API.
 */
public class ItSqlClientSynchronousApiTest extends ItSqlSynchronousApiTest {
    private IgniteClient client;

    private static final int ROW_COUNT = 16;

    @BeforeAll
    public void startClient() {
        client = IgniteClient.builder().addresses(getClientAddresses(CLUSTER_NODES).get(0)).build();
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
    @Test
    public void dml() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        checkDml(ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");
    }
}
