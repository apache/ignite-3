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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests to verify parsing of comments.
 *
 * <p>Covers:<ol>
 * <li>E161: SQL comments using leading double minus</li>
 * </ol>
 *
 * <p>According to SQL standard, SQL text containing one or more instances of comment is equivalent to the same SQL text with the comment
 * replaced with newline.
 */
public class CommentParsingTest extends AbstractParserTest {
    private static final IgniteLogger LOG = Loggers.forClass(CommentParsingTest.class);

    private static final String NL = System.lineSeparator();

    @ParameterizedTest
    @EnumSource(Statement.class)
    void leadingSimpleComment(Statement statement) {
        String originalQueryString = statement.text;
        String queryWithComment = "-- this is comment " + NL + originalQueryString;

        assertQueries(
                originalQueryString,
                queryWithComment
        );
    }

    @ParameterizedTest
    @EnumSource(Statement.class)
    void trailingSimpleComment(Statement statement) {
        String originalQueryString = statement.text;
        String queryWithComment = originalQueryString + NL + "-- this is comment";

        assertQueries(
                originalQueryString,
                queryWithComment
        );
    }

    @Test
    void emptyStatementSimpleComment() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("-- this is comment")
        );
    }

    /**
     * This test injects simple comment before random line break.
     */
    @ParameterizedTest
    @EnumSource(Statement.class)
    void infixSimpleComment(Statement statement) {
        int iterations = 50;
        long seed = ThreadLocalRandom.current().nextLong();

        LOG.info("Seed is {}", seed);

        Random rnd = new Random(seed);

        // it's well-known query that has less than
        // Integer.MAX_VALUE lines
        @SuppressWarnings("NumericCastThatLosesPrecision")
        int linesCount = (int) statement.text.lines().count();

        for (int i = 0; i < iterations; i++) {
            int lineToInject = Integer.min(statement.maxLineBreak, rnd.nextInt(linesCount));

            int lineNum = 0;
            IgniteStringBuilder sb = new IgniteStringBuilder();
            for (String line : statement.text.split(NL)) {
                sb.app(line);

                if (lineNum++ == lineToInject) {
                    sb.app(" -- this is simple comment");
                }

                sb.app(NL);
            }

            assertQueries(statement.text, sb.toString());
        }
    }

    private void assertQueries(String expected, String actual) {
        SqlNode expectedAst;
        SqlNode actualAst;
        try {
            expectedAst = parse(expected);
        } catch (RuntimeException ex) {
            System.err.println(expected);

            throw ex;
        }

        try {
            actualAst = parse(actual);
        } catch (RuntimeException ex) {
            System.err.println(expected);

            throw ex;
        }

        assertEquals(unparse(expectedAst), unparse(actualAst));
    }

    private enum Statement {
        QUERY(Q15_STRING),

        DML_INSERT("INSERT", "INTO", "t", Q15_STRING),

        DML_UPDATE("UPDATE", "t", "SET", "a=1", "WHERE", "EXISTS(" + Q15_STRING + ")"),

        DML_DELETE("DELETE", "FROM", "t", "WHERE", "EXISTS(" + Q15_STRING + ")"),

        DDL("CREATE", "TABLE", "t", "(", "id", "INT", "PRIMARY", "KEY", ",", "val VARCHAR", "NOT", "NULL",
                ",", "PRIMARY", "KEY (", "id", ")", ")", "WITH", "PRIMARY_ZONE", "=", "mZone"),

        EXPLAIN("EXPLAIN", "PLAN", "FOR", Q15_STRING),

        TX("START", "TRANSACTION")
        ;

        private final int maxLineBreak;
        private final String text;

        Statement(String... queryTokes) {
            this.text = String.join(NL, queryTokes);
            this.maxLineBreak = queryTokes.length;
        }
    }

    // This is q15 from TPC-H. Didn't use TpchHelper here on purpose to make sure test
    // won't be affected by accident change in query string. The test is sensible even to
    // particular formatting
    @SuppressWarnings("ConcatenationWithEmptyString")
    private static final String Q15_STRING = ""
            + "WITH revenue (supplier_no, total_revenue) as (" + NL
            + "  SELECT" + NL
            + "    l_suppkey," + NL
            + "    sum(l_extendedprice * (1-l_discount))" + NL
            + "  FROM" + NL
            + "    lineitem" + NL
            + "  WHERE" + NL
            + "    l_shipdate >= DATE '1996-01-01'" + NL
            + "    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH" + NL
            + "  GROUP BY" + NL
            + "    l_suppkey" + NL
            + ")" + NL
            + "SELECT" + NL
            + "  s_suppkey," + NL
            + "  s_name," + NL
            + "  s_address," + NL
            + "  s_phone," + NL
            + "  total_revenue" + NL
            + "FROM" + NL
            + "  supplier," + NL
            + "  revenue" + NL
            + "WHERE" + NL
            + "  s_suppkey = supplier_no" + NL
            + "  AND total_revenue = (" + NL
            + "    SELECT" + NL
            + "      max(total_revenue)" + NL
            + "    FROM" + NL
            + "      revenue" + NL
            + ")" + NL
            + "ORDER BY" + NL
            + "  s_suppkey";
}
