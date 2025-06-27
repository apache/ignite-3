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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for KILL SQL statement. */
public class SqlKillParserTest extends AbstractParserTest {

    /** KILL statement. */
    @ParameterizedTest
    @CsvSource({
            "KILL QUERY 'abc', QUERY",
            "KILL TRANSACTION 'abc', TRANSACTION",
            "KILL COMPUTE 'abc', COMPUTE",
    })
    public void killStatement(String stmt, IgniteSqlKillObjectType objectType) {
        SqlNode sqlNode = parse(stmt);

        IgniteSqlKill kill = assertInstanceOf(IgniteSqlKill.class, sqlNode);

        assertEquals("abc", kill.objectId().toValue());
        assertEquals(objectType, kill.objectType());
        assertNull(kill.noWait());

        expectUnparsed(kill, stmt);
    }

    /** KILL .. NO WAIT statement. */
    @ParameterizedTest
    @CsvSource({
            "KILL QUERY 'abc' NO WAIT, QUERY",
            "KILL TRANSACTION 'abc' NO WAIT, TRANSACTION",
            "KILL COMPUTE 'abc' NO WAIT, COMPUTE",
    })
    public void killStatementNoWait(String stmt, IgniteSqlKillObjectType objectType) {
        SqlNode sqlNode = parse(stmt);

        IgniteSqlKill kill = assertInstanceOf(IgniteSqlKill.class, sqlNode);

        assertEquals("abc", kill.objectId().toValue());
        assertEquals(objectType, kill.objectType());
        assertEquals(Boolean.TRUE, kill.noWait());

        expectUnparsed(kill, stmt);
    }

    /** KILL statement with incorrect object ids. */
    @ParameterizedTest
    @ValueSource(strings = {
            "KILL QUERY 1",
            "KILL QUERY 1.1",
            "KILL QUERY 1 NO WAIT",
            "KILL QUERY 'id' NO WAIT NO WAIT",

            "KILL TRANSACTION 1",
            "KILL TRANSACTION 1",
            "KILL TRANSACTION 1.1 NO WAIT",
            "KILL TRANSACTION 'id' NO WAIT NO WAIT",

            "KILL COMPUTE 1",
            "KILL COMPUTE 1.1",
            "KILL COMPUTE 1 NO WAIT",
            "KILL COMPUTE 'id' NO WAIT NO WAIT",
    })
    public void killStatementNonStringObjectId(String stmt) {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"",
                () -> parse(stmt)
        );
    }
}
