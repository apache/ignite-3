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

import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Tests client handler metrics.
 */
public class MetricsTest extends AbstractClientTest {
    @Test
    public void testTxMetrics() {
        assertEquals(0, testServer.metrics().transactionsActive().value());

        Transaction tx1 = client.transactions().begin();
        assertEquals(1, testServer.metrics().transactionsActive().value());

        Transaction tx2 = client.transactions().begin();
        assertEquals(2, testServer.metrics().transactionsActive().value());

        tx1.rollback();
        assertEquals(1, testServer.metrics().transactionsActive().value());

        tx2.rollback();
        assertEquals(0, testServer.metrics().transactionsActive().value());
    }

    @Test
    public void testSqlMetrics() {
        Statement statement = client.sql().statementBuilder()
                .property("hasMorePages", true)
                .query("select 1")
                .build();

        assertEquals(0, testServer.metrics().cursorsActive().value());

        try (Session session = client.sql().createSession()) {
            ResultSet<SqlRow> resultSet = session.execute(null, statement);
            assertEquals(1, testServer.metrics().cursorsActive().value());

            resultSet.close();
            assertEquals(0, testServer.metrics().cursorsActive().value());
        }
    }
}
