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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify e2e cases of optimized lookup by primary key.
 */
public class ItSqlUsesKeyValueGetTest extends BaseSqlIntegrationTest {
    private static final int TABLE_SIZE = 10;

    @BeforeAll
    @SuppressWarnings({"ConcatenationWithEmptyString", "resource"})
    static void initSchema() {
        CLUSTER.aliveNode().sql().executeScript(""
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
    void lookupBySimpleKey() {
        int key = randomKey();

        assertQuery("SELECT * FROM simple_key WHERE id = ?")
                .matches(containsSubPlan("KeyValueGet"))
                .withParams(key)
                .returns(key, key)
                .check();
    }

    @RepeatedTest(3)
    void lookupByComplexNormalKey() {
        int key = randomKey();

        assertQuery("SELECT * FROM complex_key_normal_order WHERE id1 = ? AND id2 = ?")
                .matches(containsSubPlan("KeyValueGet"))
                .withParams(key, 2 * key)
                .returns(key, 2 * key, key)
                .check();
    }

    @RepeatedTest(3)
    void lookupByComplexReversedKey() {
        int key = randomKey();

        assertQuery("SELECT * FROM complex_key_revers_order WHERE id1 = ? AND id2 = ?")
                .matches(containsSubPlan("KeyValueGet"))
                .withParams(key, 2 * key)
                .returns(key, 2 * key, key)
                .check();
    }

    @Test
    void lookupBySimpleKeyWithPostFiltration() {
        assertQuery("SELECT * FROM simple_key WHERE id = ? AND val > 5")
                .matches(containsSubPlan("KeyValueGet"))
                .withParams(1)
                .returnNothing()
                .check();

        assertQuery("SELECT * FROM simple_key WHERE id = ? AND val > 5")
                .matches(containsSubPlan("KeyValueGet"))
                .withParams(6)
                .returns(6, 6)
                .check();
    }

    @Test
    void lookupBySimpleKeyWithProjection() {
        int key = randomKey();

        assertQuery("SELECT val FROM simple_key WHERE id = ?")
                .matches(containsSubPlan("KeyValueGet"))
                .withParams(key)
                .returns(key)
                .check();

        assertQuery("SELECT id, val * 10 FROM simple_key WHERE id = ?")
                .matches(containsSubPlan("KeyValueGet"))
                .withParams(key)
                .returns(key, key * 10)
                .check();
    }

    private static int randomKey() {
        int key = ThreadLocalRandom.current().nextInt(TABLE_SIZE) + 1;

        System.out.println("Key is " + key);

        return key;
    }
}
