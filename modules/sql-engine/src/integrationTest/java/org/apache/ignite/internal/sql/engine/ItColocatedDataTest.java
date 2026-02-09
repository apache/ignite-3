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

import static org.hamcrest.Matchers.not;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests colocated data. */
public class ItColocatedDataTest extends BaseSqlIntegrationTest {
    @BeforeAll
    public static void beforeTestsStarted() {
        //noinspection ConcatenationWithEmptyString
        sqlScript(""
                + "CREATE TABLE T1 (id INT PRIMARY KEY, c1 INT);"
                + "CREATE TABLE T2 (id INT PRIMARY KEY, c1 INT);"
                + "CREATE TABLE TC1 (id INT, c1 INT, PRIMARY KEY(id, c1)) COLOCATE BY (c1);"
                + "CREATE TABLE TC2 (id INT, c2 INT, PRIMARY KEY(id, c2)) COLOCATE BY (c2);"
                + "CREATE ZONE IF NOT EXISTS ZONE_TEST (PARTITIONS 1, REPLICAS 1) STORAGE PROFILES ['default'];"
                + "CREATE TABLE TC2Z (id INT, c2 INT, PRIMARY KEY(id, c2)) COLOCATE BY (c2) ZONE ZONE_TEST;");
    }

    @AfterAll
    public void stopClient() {
        //noinspection ConcatenationWithEmptyString
        sqlScript(""
                + "DROP TABLE IF EXISTS T1;"
                + "DROP TABLE IF EXISTS T2;"
                + "DROP TABLE IF EXISTS TC1;"
                + "DROP TABLE IF EXISTS TC2;"
                + "DROP TABLE IF EXISTS TC2Z;"
                + "DROP ZONE IF EXISTS ZONE_TEST;");
    }

    @ParameterizedTest
    @EnumSource(DisabledJoinRules.class)
    public void joinColocatedImplicitly(DisabledJoinRules rules) {
        assertQuery("SELECT * FROM T1 JOIN T2 USING (id)", rules.disabledRules)
                .matches(QueryChecker.matchesOnce("Exchange"))
                .matches(QueryChecker.matches("^Exchange.*Join.*"))
                .check();
    }

    @ParameterizedTest
    @EnumSource(DisabledJoinRules.class)
    public void joinColocatedExplicitly(DisabledJoinRules rules) {
        assertQuery("SELECT * FROM TC1 JOIN TC2 ON TC1.c1 = TC2.c2", rules.disabledRules)
                .matches(QueryChecker.matchesOnce("Exchange"))
                .matches(QueryChecker.matches("^Exchange.*Join.*"))
                .check();

        assertQuery("SELECT * FROM TC1 JOIN TC2 ON TC1.c1 = TC2.c2 AND TC1.id = 1", rules.disabledRules)
                .matches(QueryChecker.matchesOnce("Exchange"))
                .matches(QueryChecker.matches("^Exchange.*Join.*"))
                .check();
    }

    @ParameterizedTest
    @EnumSource(DisabledJoinRules.class)
    public void joinNonColocated(DisabledJoinRules rules) {
        assertQuery("SELECT * FROM TC1 JOIN TC2Z ON TC1.c1 = TC2Z.c2", rules.disabledRules)
                .matches(not(QueryChecker.matchesOnce("Exchange")))
                .matches(QueryChecker.matches(".*Join.*Exchange.*"))
                .check();

        assertQuery("SELECT * FROM TC1 JOIN TC2Z ON TC1.c1 = TC2Z.id", rules.disabledRules)
                .matches(not(QueryChecker.matchesOnce("Exchange")))
                .matches(QueryChecker.matches(".*Join.*Exchange.*"))
                .check();
    }

    /**
     * Join type.
     */
    public enum DisabledJoinRules {
        NESTED_LOOP(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "MergeJoinConverter",
                "HashJoinConverter"
        ),

        MERGE(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "HashJoinConverter"
        ),

        HASH(
                "MergeJoinConverter",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin"
        );

        private final String[] disabledRules;

        DisabledJoinRules(String... disabledRules) {
            this.disabledRules = disabledRules;
        }
    }
}
