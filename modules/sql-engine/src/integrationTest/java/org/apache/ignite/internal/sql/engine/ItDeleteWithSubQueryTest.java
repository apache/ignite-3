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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.matchesOnce;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Test in this class requires special conditions to met to highlight the problem.
 *
 * <p>First condition is operation must be in RW transaction. Second, custer should be of
 * size of a single node, so transaction finalisation doesn't involve any network calls.
 * Last but not least, operation must involve MergeJoin node which is source of problem. 
 */
public class ItDeleteWithSubQueryTest extends BaseSqlIntegrationTest {

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    public void dropTables() {
        dropAllTables();
    }

    @Test
    void test() {
        //noinspection ConcatenationWithEmptyString
        sqlScript("" 
                + "CREATE TABLE t1 (id INT PRIMARY KEY, val INT);"
                + "CREATE TABLE t2 (id INT PRIMARY KEY, val INT);"
        );

        for (int i = 0; i < 100; i++) {
            assertQuery("DELETE FROM t1 WHERE id IN (SELECT id FROM t2 WHERE val > 10)")
                    .matches(matchesOnce("MergeJoin"))
                    .returns(0L)
                    .check();
        }
    }
}
