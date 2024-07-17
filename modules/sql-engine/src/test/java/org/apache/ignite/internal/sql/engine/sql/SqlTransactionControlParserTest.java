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
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for transaction control SQL statements.
 */
public class SqlTransactionControlParserTest extends AbstractParserTest {

    @ParameterizedTest
    @CsvSource({
            "START TRANSACTION READ ONLY,READ_ONLY",
            "START TRANSACTION READ WRITE,READ_WRITE",
            "START TRANSACTION,IMPLICIT_READ_WRITE",
    })
    public void testStartTx(String sqlStmt, IgniteSqlStartTransactionMode mode) {
        SqlNode node = parse(sqlStmt);
        IgniteSqlStartTransaction start = assertInstanceOf(IgniteSqlStartTransaction.class, node);
        assertEquals(mode, start.getMode());

        expectUnparsed(node, sqlStmt);
    }

    @Test
    public void testStartRejectInvalid() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, " Encountered \"XY\"", () -> parse("START TRANSACTION XY"));
    }

    @Test
    public void testCommit() {
        String sql = "COMMIT";

        SqlNode node = parse(sql);
        assertInstanceOf(IgniteSqlCommitTransaction.class, node);

        expectUnparsed(node, sql);
    }
}
