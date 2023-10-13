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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import org.apache.ignite.Ignite;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for multi-statements.
 */
public class ItMultistatementTest extends ClusterPerClassIntegrationTest {

    /**
     * Transaction control statements can not be used in explicit transactions.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "START TRANSACTION",
            "START TRANSACTION READ WRITE",
            "START TRANSACTION READ ONLY",
            "COMMIT",
    })
    public void testTxControlStatementsAreNotAllowdWithExplicitTransactions(String stmtSql) {
        Ignite ignite = CLUSTER_NODES.get(0);
        IgniteTransactions transactions = ignite.transactions();
        IgniteSql igniteSql = ignite.sql();

        Transaction tx = transactions.begin();

        try (Session session = igniteSql.createSession()) {
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Transaction control statements can not used in explicit transactions",
                    () -> session.execute(tx, stmtSql));
        }
    }

    /**
     * Transaction control statements can not be used in single statement methods.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "START TRANSACTION",
            "START TRANSACTION READ WRITE",
            "START TRANSACTION READ ONLY",
            "COMMIT",
    })
    public void testTxControlStatementsAreNotAllowdWithExplicitTransactions(String stmtSql) {
        Ignite ignite = CLUSTER_NODES.get(0);
        IgniteSql igniteSql = ignite.sql();

        try (Session session = igniteSql.createSession()) {
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Transaction control statement can not be executed as an independent statement",
                    () -> session.execute(null, stmtSql));
        }
    }
}
