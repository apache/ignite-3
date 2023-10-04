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

package org.apache.ignite.internal.sql.engine.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for other SQL statements.
 */
public class SqlOtherStatementsTest {

    @Test
    public void testStartReadOnlyTx() {
        String sql = "START TRANSACTION READ ONLY";

        SqlNode node = parseStatement(sql);
        IgniteSqlStartTransaction start = assertInstanceOf(IgniteSqlStartTransaction.class, node);
        assertTrue(start.isReadOnly());

        expectUnparsed(node, sql);
    }

    @Test
    public void testStartReadWriteTx() {
        String sql = "START TRANSACTION READ WRITE";

        SqlNode node = parseStatement(sql);
        IgniteSqlStartTransaction start = assertInstanceOf(IgniteSqlStartTransaction.class, node);
        assertFalse(start.isReadOnly());

        expectUnparsed(node, sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "START TRANSACTION",
            "START TRANSACTION READ",
            "START TRANSACTION WRITE",
            "START TRANSACTION XYZ",
    })
    public void testStartRejectInvalid(String sqlStmt) {
        assertThrows(SqlException.class, () -> parseStatement(sqlStmt));
    }

    @Test
    public void testCommit() {
        String sql = "COMMIT";

        SqlNode node = parseStatement(sql);
        assertInstanceOf(IgniteSqlCommitTransaction.class, node);

        expectUnparsed(node, sql);
    }

    private static SqlNode parseStatement(String sqlStmt) {
        return IgniteSqlParser.parse(sqlStmt, StatementParseResult.MODE).statement();
    }

    private static void expectUnparsed(SqlNode node, String expected) {
        SqlPrettyWriter writer = new SqlPrettyWriter();
        node.unparse(writer, 0, 0);

        assertEquals(expected, writer.toString(), "Unparse does not match");
    }
}
