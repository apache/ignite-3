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

package org.apache.ignite.distributed;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

class ItTableScanTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testScanOrderIsPreservedWithWriteIntents() {
        sql(String.format("CREATE ZONE %s WITH STORAGE_PROFILES='default', PARTITIONS = 1", ZONE_NAME));
        sql(String.format("CREATE TABLE %s (id INT PRIMARY KEY, val VARCHAR) ZONE %s", TABLE_NAME, ZONE_NAME));

        sql(String.format("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME));
        sql(String.format("INSERT INTO %s VALUES (2, 'b')", TABLE_NAME));
        sql(String.format("INSERT INTO %s VALUES (3, 'c')", TABLE_NAME));

        List<List<Object>> originalResult = sql(String.format("SELECT * FROM %s", TABLE_NAME));

        IntStream.rangeClosed(1, 3).forEach(i -> {
            runInTransaction(tx -> {
                // Update a row in a separate transaction. This creates a write intent that needs to be resolved when we read data from the
                // other, implicit, transaction.
                sql(tx, String.format("UPDATE %S SET val = 'd' WHERE id = %d", TABLE_NAME, i));

                validateOrder(originalResult);
            });
        });

        // Test all combinations of 2 write intents.
        IntStream.rangeClosed(1, 3).forEach(i -> {
            IntStream.rangeClosed(i + 1, 3).forEach(j -> {
                runInTransaction(tx -> {
                    sql(tx, String.format("UPDATE %S SET val = 'd' WHERE id = %d", TABLE_NAME, i));
                    sql(tx, String.format("UPDATE %S SET val = 'd' WHERE id = %d", TABLE_NAME, j));

                    validateOrder(originalResult);
                });
            });
        });

        // Test order when all entries are write intents.
        runInTransaction(tx -> {
            sql(tx, String.format("UPDATE %S SET val = 'd' WHERE id = %d", TABLE_NAME, 1));
            sql(tx, String.format("UPDATE %S SET val = 'd' WHERE id = %d", TABLE_NAME, 2));
            sql(tx, String.format("UPDATE %S SET val = 'd' WHERE id = %d", TABLE_NAME, 3));

            validateOrder(originalResult);
        });
    }

    private static void runInTransaction(Consumer<Transaction> action) {
        Transaction tx = node(0).transactions().begin();

        try {
            action.accept(tx);
        } finally {
            tx.rollback();
        }
    }

    private static void validateOrder(List<List<Object>> originalResult) {
        List<List<Object>> resultWithWriteIntent = sql(String.format("SELECT * FROM %s", TABLE_NAME));

        assertThat(resultWithWriteIntent, is(equalTo(originalResult)));
    }
}
