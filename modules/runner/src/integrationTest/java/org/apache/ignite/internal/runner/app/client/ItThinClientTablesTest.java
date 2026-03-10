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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests for thin client table operations.
 */
@SuppressWarnings("resource")
public class ItThinClientTablesTest extends ItAbstractThinClientTest {
    private static final String TEST_TABLE = "TEST_TABLE";
    private static final String TEST_ZONE = "TEST_PARTITION_ZONE";

    @Test
    public void testTablePartitionCountChange() {
        IgniteClient client = client();
        IgniteSql sql = server().sql();

        // Create zone and table with 5 partitions
        sql.execute("CREATE ZONE " + TEST_ZONE + " (REPLICAS " + replicas() + ", PARTITIONS 5) STORAGE PROFILES ['"
                + DEFAULT_STORAGE_PROFILE + "']");
        sql.execute("CREATE TABLE " + TEST_TABLE + "(id INT PRIMARY KEY, val VARCHAR) ZONE " + TEST_ZONE);

        // Perform operations to ensure client caches partition info
        Table table = client.tables().table(TEST_TABLE);
        assertNotNull(table);

        RecordView<Tuple> view = table.recordView();
        Tuple key1 = Tuple.create().set("id", 1);
        Tuple record1 = Tuple.create().set("id", 1).set("val", "value1");
        view.upsert(null, record1);

        Tuple result = view.get(null, key1);
        assertNotNull(result);
        assertEquals("value1", result.stringValue("val"));

        // Drop the table and zone
        sql.execute("DROP TABLE " + TEST_TABLE);
        sql.execute("DROP ZONE " + TEST_ZONE);

        // Recreate zone with different partition count (10 partitions) and table with same name
        sql.execute("CREATE ZONE " + TEST_ZONE + " (REPLICAS " + replicas() + ", PARTITIONS 10) STORAGE PROFILES ['"
                + DEFAULT_STORAGE_PROFILE + "']");
        sql.execute("CREATE TABLE " + TEST_TABLE + "(id INT PRIMARY KEY, val VARCHAR) ZONE " + TEST_ZONE);

        // Get the table again - this should refresh cached metadata
        table = client.tables().table(TEST_TABLE);
        assertNotNull(table);

        // Perform operations on the recreated table - this verifies client handles new partition count correctly
        view = table.recordView();
        Tuple key2 = Tuple.create().set("id", 2);
        Tuple record2 = Tuple.create().set("id", 2).set("val", "value2");
        view.upsert(null, record2);

        result = view.get(null, key2);
        assertNotNull(result);
        assertEquals("value2", result.stringValue("val"));

        // Verify the old key doesn't exist (table was recreated)
        result = view.get(null, key1);
        assertNull(result);

        // Clean up
        sql.execute("DROP TABLE " + TEST_TABLE);
        sql.execute("DROP ZONE " + TEST_ZONE);
    }
}
