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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_PARSE_ERR;

import java.io.Reader;
import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.util.SourceStringReader;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImplConstants;
import org.apache.ignite.internal.generated.query.calcite.sql.ParseException;
import org.apache.ignite.internal.generated.query.calcite.sql.Token;
import org.apache.ignite.internal.generated.query.calcite.sql.TokenMgrError;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.sql.SqlException;

/**
 * Provides method for parsing SQL statements in SQL dialect of Apache Ignite 3.
 *
 * <p>One should use parsing methods defined in this class,
 * instead of creating {@link SqlParser} that use {@link IgniteSqlParserImpl} directly.
 */
public final class IgniteSqlParser  {
    /**
     * Parser configuration.
     */
    public static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withParserFactory(InternalIgniteSqlParser.FACTORY)
            .withLex(Lex.ORACLE)
            // Do not validate the length SQL identifiers.
            .withIdentifierMaxLength(Integer.MAX_VALUE)
            .withConformance(IgniteSqlConformance.INSTANCE);

    private IgniteSqlParser() {

    }

    /**
     * Parses the given SQL string in the specified {@link ParseMode mode},
     * which determines the result of the parse operation.
     *
     * @param sql  An SQL string.
     * @param mode  A parse mode.
     * @return  A parse result.
     *
     * @see StatementParseResult#MODE
     * @see ScriptParseResult#MODE
     */
    public static <T extends ParseResult> T parse(String sql, ParseMode<T> mode) {
        try (SourceStringReader reader = new SourceStringReader(sql)) {
            return parse(reader, mode);
        }
    }

    /**
     * Parses an SQL string from the given reader in the specified {@link ParseMode mode},
     * which determines the result of the parse operation.
     *
     * @param reader  A read that contains an SQL string.
     * @param mode  A parse mode.
     * @return  A parse result.
     *
     * @see StatementParseResult#MODE
     * @see ScriptParseResult#MODE
     */
    public static <T extends ParseResult> T parse(Reader reader, ParseMode<T> mode) {
        try  {
            InternalIgniteSqlParser.dynamicParamCount.set(null);

            SqlParser parser = SqlParser.create(reader, PARSER_CONFIG);
            SqlNodeList nodeList = parser.parseStmtList();

            Integer dynamicParamsCount = InternalIgniteSqlParser.dynamicParamCount.get();
            assert dynamicParamsCount != null : "dynamicParamCount has not been updated";

            List<SqlNode> list = nodeList.getList();

            for (int i = 0; i < list.size(); i++) {
                SqlNode original = list.get(i);
                SqlNode node = fixNodesIfNecessary(original);
                list.set(i, node);
            }

            return mode.createResult(list, dynamicParamsCount);
        } catch (SqlParseException e) {
            throw convertException(e);
        } finally {
            InternalIgniteSqlParser.dynamicParamCount.set(null);
        }
    }

    /**
     * Converts the given exception to the {@link SqlException}.
     *
     * <p>Besides converting, cut out part of the message from original exception with
     * suggested options. The reason to do that is currently grammar of the parser is not aligned
     * with Ignite's capabilities. As a result, the message may contain misleading options. Another
     * problem is that message sometimes may contain almost every keyword, bloating the message up
     * to hundreds of lines (given that every keyword is on a new line).
     *
     * @param ex An exception to convert.
     * @return An instance of SqlException.
     */
    private static SqlException convertException(SqlParseException ex) {
        Throwable cause = ex.getCause();

        String message;
        if (cause instanceof ParseException) {
            // ParserException means that given query cannot be parsed according to grammar rules.
            // Let's extract token that violates grammar and point out to this token in error message.
            ParseException parserEx = (ParseException) cause;

            if (parserEx.currentToken == null) {
                // if currentToken is null, the exception was replaced by one created manually
                // in IgniteSqlParserImpl.convertException. There is not much we can do,
                // thus let's just cut out tail with suggested keywords

                String originalMessage = parserEx.getMessage();

                // Example:
                //     Incorrect syntax near the keyword 'FROM' at line 1, column 12.\nWas expecting one of:
                int endOfTheFirstSentence = originalMessage.indexOf(".\n");

                message = originalMessage.substring(0, endOfTheFirstSentence);
            } else {
                // currentToken is the one that has been "consumed" last. Token is consumed
                // only if it satisfies the grammar, thus token in question is the one that
                // following next
                Token tokenInQuestion = parserEx.currentToken.next;

                String tokenImage;
                // some tokens (for example, <EOF> - End Of File) don't have particular image 
                // in query string, but has conventional designation in 
                // IgniteSqlParserImplConstants#tokenImage
                if (StringUtils.nullOrBlank(tokenInQuestion.image)) {
                    tokenImage = IgniteSqlParserImplConstants.tokenImage[tokenInQuestion.kind];
                } else {
                    tokenImage = tokenInQuestion.image;
                }

                message = format(
                        "Encountered \"{}\" at line {}, column {}",
                        tokenImage,
                        tokenInQuestion.beginLine,
                        tokenInQuestion.beginColumn
                );
            }
        } else if (cause instanceof TokenMgrError) {
            // TokenMgrError generally means, that token manager was unable to split given query
            // by tokens. The one example is SELECT foo#bar.
            // This exception already contains necessary context including position in query
            // string, thus let's just reuse message as is
            message = cause.getMessage();
        } else {
            // here we are going to handle any exception thrown by validator. A few examples:
            //     - `cause` will be CalciteException if parser found expression (like, `a=2`)
            //        where query is expected
            //     - `cause` will be SqlValidatorException if parser was unable to parse decimal literal
            //
            // Such exceptions don't contain information about position in query string, thus let's
            // attach it at the end of the message

            String messageFromCause = cause.getMessage().trim();

            if (messageFromCause.endsWith(".")) {
                messageFromCause = messageFromCause.substring(0, messageFromCause.length() - 1);
            }

            message = format(
                    "{}. At line {}, column {}",
                    messageFromCause,
                    ex.getPos().getLineNum(),
                    ex.getPos().getColumnNum()
            );
        }

        return new SqlException(STMT_PARSE_ERR, "Failed to parse query: " + message);
    }

    private static final class InternalIgniteSqlParser extends IgniteSqlParserImpl {

        /**
         * A factory that create instances of {@link IgniteSqlParser}.
         */
        private static final SqlParserImplFactory FACTORY = new SqlParserImplFactory() {
            @Override
            public SqlAbstractParserImpl getParser(Reader reader) {
                InternalIgniteSqlParser parser = new InternalIgniteSqlParser(reader);
                if (reader instanceof SourceStringReader) {
                    String sql = ((SourceStringReader) reader).getSourceString();
                    parser.setOriginalSql(sql);
                }
                return parser;
            }
        };


        // We store the number of dynamic parameters in a thread local since
        // it is not possible to access an instance of IgniteSqlParser created by a parser factory.
        static final ThreadLocal<Integer> dynamicParamCount = new ThreadLocal<>();

        InternalIgniteSqlParser(Reader reader) {
            super(reader);
        }

        /** {@inheritDoc} **/
        @Override
        public SqlNode parseSqlExpressionEof() throws Exception {
            try {
                return super.parseSqlExpressionEof();
            } finally {
                dynamicParamCount.set(nDynamicParams);
            }
        }

        /** {@inheritDoc} **/
        @Override
        public SqlNode parseSqlStmtEof() throws Exception {
            try {
                return super.parseSqlStmtEof();
            } finally {
                dynamicParamCount.set(nDynamicParams);
            }
        }

        /** {@inheritDoc} **/
        @Override
        public SqlNodeList parseSqlStmtList() throws Exception {
            try {
                return super.parseSqlStmtList();
            } finally {
                dynamicParamCount.set(nDynamicParams);
            }
        }
    }

    private static SqlNode fixNodesIfNecessary(SqlNode node) {
        // Create copies of DML nodes because original use incomplete implementation
        // of SqlOperator that does not provide implementation of createCall method.
        if (node.getKind() == SqlKind.DELETE) {
            return new IgniteSqlDelete((SqlDelete) node);
        } else if (node.getKind() == SqlKind.UPDATE) {
            return new IgniteSqlUpdate((SqlUpdate) node);
        } else if (node.getKind() == SqlKind.MERGE) {
            return new IgniteSqlMerge((SqlMerge) node);
        } else {
            return node;
        }
    }
}
