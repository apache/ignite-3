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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for other SQL statements.
 */
public class SqlOtherStatementsTest {

    @ParameterizedTest
    @CsvSource({
            "START TRANSACTION READ ONLY,READ_ONLY",
            "START TRANSACTION READ WRITE,READ_WRITE",
            "START TRANSACTION,IMPLICIT_READ_WRITE",
    })
    public void testStartTx(String sqlStmt, IgniteSqlStartTransactionMode mode) {
        SqlNode node = parseStatement(sqlStmt);
        IgniteSqlStartTransaction start = assertInstanceOf(IgniteSqlStartTransaction.class, node);
        assertEquals(mode, start.getMode());

        expectUnparsed(node, sqlStmt);
    }

    @Test
    public void testStartRejectInvalid() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, " Encountered \"XY\"", () -> parseStatement("START TRANSACTION XY"));
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
