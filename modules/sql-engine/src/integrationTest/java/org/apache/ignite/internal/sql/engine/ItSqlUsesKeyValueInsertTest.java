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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify e2e cases of optimized insert.
 */
public class ItSqlUsesKeyValueInsertTest extends BaseSqlIntegrationTest {
    private static final int TABLE_SIZE = 10;

    @BeforeAll
    @SuppressWarnings("ConcatenationWithEmptyString")
    static void initSchema() {
        CLUSTER.aliveNode().sql().executeScript(""
                + "CREATE TABLE simple_key (id INT PRIMARY KEY, val INT DEFAULT 42);"
                + "CREATE TABLE complex_key (id1 INT, id2 INT, val INT, PRIMARY KEY(id1, id2));"
        );
    }

    @AfterEach
    void clearTables() {
        sql("DELETE FROM simple_key");
        sql("DELETE FROM complex_key");
    }

    @Test
    void insertConstantSimpleKey() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery(format("INSERT INTO simple_key VALUES ({}, {})", i, i))
                    .matches(containsSubPlan("KeyValueModify"))
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT * FROM simple_key WHERE id = ?")
                    .withParams(i)
                    .returns(i, i)
                    .check();
        }
    }

    @Test
    void insertConstantReversFieldOrder() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery(format("INSERT INTO simple_key (val, id) VALUES ({}, {})", i * 10, i))
                    .matches(containsSubPlan("KeyValueModify"))
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT * FROM simple_key WHERE id = ?")
                    .withParams(i)
                    .returns(i, i * 10)
                    .check();
        }
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    void insertWithImplicitPk() {
        sql("CREATE TABLE implicit_pk(val INT)");

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery(format("INSERT INTO implicit_pk VALUES ({})", i))
                    .matches(containsSubPlan("KeyValueModify"))
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT true FROM implicit_pk WHERE val = ?")
                    .withParams(i)
                    .returns(true)
                    .check();
        }
    }

    @Test
    void insertWithDefault() {
        // implicit default
        for (int i = 0; i < TABLE_SIZE / 2; i++) {
            assertQuery(format("INSERT INTO simple_key (id) VALUES ({})", i))
                    .matches(containsSubPlan("KeyValueModify"))
                    .returns(1L)
                    .check();
        }

        // explicit default
        for (int i = TABLE_SIZE / 2; i < TABLE_SIZE; i++) {
            assertQuery(format("INSERT INTO simple_key (id, val) VALUES ({}, DEFAULT)", i))
                    .matches(containsSubPlan("KeyValueModify"))
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT * FROM simple_key WHERE id = ?")
                    .withParams(i)
                    .returns(i, 42)
                    .check();
        }
    }

    @Test
    void insertDynamicParamsSimpleKey() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("INSERT INTO simple_key VALUES (?, ?)")
                    .matches(containsSubPlan("KeyValueModify"))
                    .withParams(i, i)
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT * FROM simple_key WHERE id = ?")
                    .withParams(i)
                    .returns(i, i)
                    .check();
        }
    }

    @Test
    void insertExpressionSimpleKey() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("INSERT INTO simple_key VALUES (?, CASE WHEN ? % 2 = 0 THEN ? * 3 ELSE ? * 2 END)")
                    .withParams(i, /* case predicate */ i, /* then branch */ i, /* else branch */ i)
                    .matches(containsSubPlan("KeyValueModify"))
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT * FROM simple_key WHERE id = ?")
                    .withParams(i)
                    .returns(i, i % 2 == 0 ? i * 3 : i * 2)
                    .check();
        }
    }

    @Test
    void insertSimpleKeyWithCast() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("INSERT INTO simple_key VALUES (?, ?)")
                    .matches(containsSubPlan("KeyValueModify"))
                    .withParams((byte) i, (byte) i)
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT * FROM simple_key WHERE id = ?")
                    .withParams(i)
                    .returns(i, i)
                    .check();
        }
    }

    @Test
    void insertComplexKey() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("INSERT INTO complex_key VALUES (?, ?, ?)")
                    .matches(containsSubPlan("KeyValueModify"))
                    .withParams(i, 2 * i, i)
                    .returns(1L)
                    .check();
        }

        for (int i = 0; i < TABLE_SIZE; i++) {
            assertQuery("SELECT * FROM complex_key WHERE id1 = ? AND id2 = ?")
                    .withParams(i, 2 * i)
                    .returns(i, 2 * i, i)
                    .check();
        }
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void exceptionIsThrownOnKeyViolation() {
        String insertStatement = "INSERT INTO simple_key VALUES (1, 1)";
        sql(insertStatement);

        assertThrows(
                SqlException.class,
                () -> sql(insertStatement),
                "PK unique constraint is violated"
        );

        assertThrows(
                SqlException.class,
                () -> sql("INSERT INTO complex_key(id1, val) VALUES (1, 1)"),
                "Column 'ID2' does not allow NULLs"
        );
    }
}
