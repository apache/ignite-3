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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify e2e cases of optimized lookup by primary key.
 */
public class ItSqlUsesKeyValueDeleteTest extends BaseSqlIntegrationTest {
    private static final int TABLE_SIZE = 10;

    @BeforeEach
    @SuppressWarnings("ConcatenationWithEmptyString")
    void initSchema() {
        CLUSTER.aliveNode().sql().executeScript(""
                + "DROP TABLE IF EXISTS simple_key;"
                + "DROP TABLE IF EXISTS complex_key_normal_order;"
                + "DROP TABLE IF EXISTS complex_key_revers_order;" 
                + ""
                + "CREATE TABLE simple_key (id INT PRIMARY KEY, val INT);"
                + "CREATE TABLE complex_key_normal_order (id1 INT, id2 INT, val INT, PRIMARY KEY(id1, id2));"
                + "CREATE TABLE complex_key_revers_order (id1 INT, id2 INT, val INT, PRIMARY KEY(id2, id1));"
                + ""
                + "INSERT INTO simple_key SELECT x, x FROM TABLE(system_range(1, ?));"
                + "INSERT INTO complex_key_normal_order SELECT x, 2 * x, x FROM TABLE(system_range(1, ?));"
                + "INSERT INTO complex_key_revers_order SELECT x, 2 * x, x FROM TABLE(system_range(1, ?));",
                TABLE_SIZE, TABLE_SIZE, TABLE_SIZE
        );
    }

    @RepeatedTest(3)
    void deleteBySimpleKey() {
        int key = randomKey();

        assertQuery("DELETE FROM simple_key WHERE id = ?")
                .matches(containsSubPlan("KeyValueModify"))
                .withParams(key)
                .returns(1L)
                .check();
    }

    @RepeatedTest(3)
    void deleteByComplexNormalKey() {
        int key = randomKey();

        assertQuery("DELETE FROM complex_key_normal_order WHERE id1 = ? AND id2 = ?")
                .matches(containsSubPlan("KeyValueModify"))
                .withParams(key, 2 * key)
                .returns(1L)
                .check();
    }

    @RepeatedTest(3)
    void deleteByComplexReversedKey() {
        int key = randomKey();

        assertQuery("DELETE FROM complex_key_revers_order WHERE id1 = ? AND id2 = ?")
                .matches(containsSubPlan("KeyValueModify"))
                .withParams(key, 2 * key)
                .returns(1L)
                .check();
    }

    @Test
    void lookupOnOutOfRangeKey() {
        Transaction tx = CLUSTER.aliveNode().transactions().begin();

        try {
            sql(tx, "INSERT INTO simple_key VALUES (2147483647, 0), (-2147483648, 0);");

            assertQuery((InternalTransaction) tx, "DELETE FROM simple_key WHERE id = 2147483648")
                    .returns(0L)
                    .check();

            assertQuery((InternalTransaction) tx, "DELETE FROM simple_key WHERE id = -2147483649")
                    .returns(0L)
                    .check();
        } finally {
            tx.rollback();
        }
    }

    private static int randomKey() {
        int key = ThreadLocalRandom.current().nextInt(TABLE_SIZE) + 1;

        System.out.println("Key is " + key);

        return key;
    }
}
