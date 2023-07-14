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

package org.apache.ignite.internal.runner.app.client;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests for client schema synchronization.
 */
public class ItThinClientSchemaSynchronizationTest extends ItAbstractThinClientTest {
    @SuppressWarnings("resource")
    @Test
    void testOutdatedSchemaFromClientThrowsExceptionOnServer() throws InterruptedException {
        IgniteClient client = client();
        Session ses = client.sql().createSession();

        // Create table, insert data.
        String tableName = "testOutdatedSchemaFromClientThrowsExceptionOnServer";
        ses.execute(null, "CREATE TABLE " + tableName + "(ID INT NOT NULL PRIMARY KEY)");

        waitForTableOnAllNodes(tableName);
        RecordView<Tuple> recordView = client.tables().table(tableName).recordView();

        Tuple rec = Tuple.create().set("ID", 1);
        recordView.insert(null, rec);

        // Modify table, get data - client will use old schema.
        ses.execute(null, "ALTER TABLE testOutdatedSchemaFromClientThrowsExceptionOnServer ADD COLUMN NAME VARCHAR");
        recordView.get(null, rec);
    }
}
