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

import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for multi-statements.
 */
public class ItMultistatementTest extends ClusterPerClassIntegrationTest {

    /**
     * Transaction control statements can not be used in single statement methods.
     */
    @ParameterizedTest
    @MethodSource("txControlCalls")
    public void testTxControlStatementsAreNotAllowedWithSingleStatementMethods(String stmtSql, ExecMethod execMethod) {
        Ignite ignite = CLUSTER.aliveNode();
        IgniteSql igniteSql = ignite.sql();

        RuntimeException t = assertThrows(RuntimeException.class, () -> execMethod.execute(igniteSql, stmtSql, null));

        String message = "Transaction control statement can not be executed as an independent statement";
        checkError(message, t);
    }

    /** Sql session method to use. */
    public enum ExecMethod {
        ASYNC,
        SYNC,

        STMT_ASYNC,
        STMT_SYNC,
        ;

        void execute(IgniteSql sql, String stmtSql, @Nullable Transaction tx) {
            switch (this) {
                case ASYNC:
                    sql.executeAsync(tx, stmtSql).join();
                    break;
                case SYNC:
                    sql.execute(tx, stmtSql).next();
                    break;
                case STMT_ASYNC: {
                    Statement s = sql.createStatement(stmtSql);
                    sql.executeAsync(tx, s).join();
                }
                    break;
                case STMT_SYNC: {
                    Statement s = sql.createStatement(stmtSql);
                    sql.execute(tx, s).next();
                }
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + this);
            }
        }
    }

    private static Stream<Arguments> txControlCalls() {
        return txControlStatements().flatMap(s -> Arrays.stream(ExecMethod.values()).map(m -> Arguments.of(s, m)));
    }

    private static Stream<String> txControlStatements() {
        return Stream.of(
                "START TRANSACTION",
                "START TRANSACTION READ WRITE",
                "START TRANSACTION READ ONLY",
                "COMMIT"
        );
    }

    private static void checkError(String message, RuntimeException err) {
        SqlException sqlException;
        if (err instanceof CompletionException) {
            sqlException = assertInstanceOf(SqlException.class, err.getCause());
        } else {
            sqlException = assertInstanceOf(SqlException.class, err);
        }

        assertEquals(STMT_VALIDATION_ERR, sqlException.code());
        assertThat(sqlException.getMessage(), containsString(message));
    }
}
