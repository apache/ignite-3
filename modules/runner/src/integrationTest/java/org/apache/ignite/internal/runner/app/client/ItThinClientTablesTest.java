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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for thin client table operations.
 */
@SuppressWarnings("resource")
public class ItThinClientTablesTest extends ItAbstractThinClientTest {
    private static final String TEST_TABLE = "TEST_TABLE";
    private static final String TEST_ZONE = "TEST_PARTITION_ZONE";

    @AfterEach
    public void afterEach() {
        IgniteSql sql = client().sql();
        sql.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
        sql.execute("DROP ZONE IF EXISTS " + TEST_ZONE);
    }

    @Test
    public void testTablePartitionCountChange() {
        IgniteClient client = client();
        IgniteSql sql = client.sql();

        // Create zone and table with 5 partitions
        sql.execute("CREATE ZONE " + TEST_ZONE + " (REPLICAS 1, PARTITIONS 5) STORAGE PROFILES ['default']");
        sql.execute("CREATE TABLE " + TEST_TABLE + "(id INT PRIMARY KEY, val VARCHAR) ZONE " + TEST_ZONE);

        // Perform operations to ensure client caches partition info
        Table table = client.tables().table(TEST_TABLE);
        assertEquals(5, table.partitionDistribution().partitions().size());

        RecordView<Tuple> view = table.recordView();
        Tuple key1 = Tuple.create().set("id", 1);
        view.upsert(null, Tuple.create().set("id", 1).set("val", "value1"));

        Tuple result = view.get(null, key1);
        assertEquals("value1", result.stringValue("val"));

        // Drop and recreate with different partition count
        sql.execute("DROP TABLE " + TEST_TABLE);
        sql.execute("DROP ZONE " + TEST_ZONE);
        sql.execute("CREATE ZONE " + TEST_ZONE + " (REPLICAS 1, PARTITIONS 10) STORAGE PROFILES ['default']");
        sql.execute("CREATE TABLE " + TEST_TABLE + "(id INT PRIMARY KEY, val VARCHAR) ZONE " + TEST_ZONE);

        // Old table handle does not work after drop.
        assertThrows(TableNotFoundException.class, () -> view.get(null, key1));

        // Get the table again.
        Table table2 = client.tables().table(TEST_TABLE);
        assertEquals(10, table2.partitionDistribution().partitions().size());

        RecordView<Tuple> view2 = table2.recordView();
        assertNull(view2.get(null, key1), "Old key should not exist after table recreation");
    }
}
