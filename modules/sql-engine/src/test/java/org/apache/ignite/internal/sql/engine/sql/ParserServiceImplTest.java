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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests to verify {@link ParserServiceImpl}.
 */
public class ParserServiceImplTest {
    enum Statement {
        QUERY("SELECT * FROM my_table", SqlQueryType.QUERY),
        DML("INSERT INTO my_table VALUES (1, 1)", SqlQueryType.DML),
        DDL("CREATE TABLE my_table (id INT PRIMARY KEY, avl INT)", SqlQueryType.DDL),
        EXPLAIN_QUERY("EXPLAIN PLAN FOR SELECT * FROM my_table", SqlQueryType.EXPLAIN),
        EXPLAIN_DML("EXPLAIN PLAN FOR INSERT INTO my_table VALUES (1, 1)", SqlQueryType.EXPLAIN);

        private final String text;
        private final SqlQueryType type;

        Statement(String text, SqlQueryType type) {
            this.text = text;
            this.type = type;
        }
    }

    @ParameterizedTest
    @EnumSource(Statement.class)
    void serviceReturnsResultOfExpectedType(Statement statement) {
        ParserServiceImpl service = new ParserServiceImpl();

        ParsedResult result = service.parse(statement.text);

        assertThat(result.queryType(), is(statement.type));
    }

    @ParameterizedTest
    @EnumSource(Statement.class)
    void resultReturnedByServiceCreateNewInstanceOfTree(Statement statement) {
        ParserServiceImpl service = new ParserServiceImpl();

        ParsedResult result = service.parse(statement.text);

        SqlNode firstCall = result.parsedTree();
        SqlNode secondCall = result.parsedTree();

        assertNotSame(firstCall, secondCall);
        assertThat(firstCall.toString(), is(secondCall.toString()));
    }

    /**
     * Checks the parsing of a query containing multiple statements.
     *
     * <p>This parsing mode is only supported using the {@link ParserService#parseScript(String)} method,
     * so {@link ParserService#parse(String)}} must fail with a validation error.
     *
     * <p>Parsing produces a list of parsing results, each of which must match the parsing
     * result of the corresponding single statement.
     */
    @Test
    void parseMultiStatementQuery() {
        ParserService service = new ParserServiceImpl();

        List<Statement> statements = List.of(Statement.values());
        IgniteStringBuilder buf = new IgniteStringBuilder();

        for (Statement statement : statements) {
            buf.app(statement.text).app(';');
        }

        String multiStatementQuery = buf.toString();

        //noinspection ThrowableNotThrown
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Multiple statements are not allowed",
                () -> service.parse(multiStatementQuery)
        );

        List<ParsedResult> results = service.parseScript(multiStatementQuery);
        assertThat(results, hasSize(statements.size()));

        for (int i = 0; i < results.size(); i++) {
            ParsedResult result = results.get(i);
            ParsedResult singleStatementResult = service.parse(statements.get(i).text);

            assertThat(result.queryType(), equalTo(statements.get(i).type));
            assertThat(result.parsedTree(), notNullValue());
            assertThat(result.parsedTree().toString(), equalTo(singleStatementResult.parsedTree().toString()));
            assertThat(result.normalizedQuery(), equalTo(singleStatementResult.normalizedQuery()));
            assertThat(result.originalQuery(), equalTo(singleStatementResult.normalizedQuery()));
        }
    }
}
