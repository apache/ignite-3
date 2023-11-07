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

import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.SqlException;

/**
 * Result of parsing SQL string that contains exactly one statement.
 */
public final class StatementParseResult extends ParseResult {

    /**
     * Expected result of a parse operation is a single statement. When there is more than one statement, parse operation fails with
     * {@link SqlException}.
     */
    public static final ParseMode<StatementParseResult> MODE = new ParseMode<>() {
        @Override
        StatementParseResult createResult(List<SqlNode> list, int dynamicParamsCount) {
            if (list.size() > 1) {
                throw new SqlException(STMT_VALIDATION_ERR, "Multiple statements are not allowed.");
            }

            return new StatementParseResult(list.get(0), dynamicParamsCount);
        }
    };

    private final SqlNode statement;

    /**
     * Constructor.
     *
     * @param sqlNode A parsed statement.
     * @param dynamicParamsCount The number of dynamic parameters.
     */
    StatementParseResult(SqlNode sqlNode, int dynamicParamsCount) {
        super(dynamicParamsCount);
        assert !(sqlNode instanceof SqlNodeList) : "Can not create a statement result from a node list: " + sqlNode;
        this.statement = sqlNode;
    }

    /** Returns a parsed statement. */
    public SqlNode statement() {
        return statement;
    }

    /** {@inheritDoc} **/
    @Override
    public String toString() {
        return S.toString(StatementParseResult.class, this, "statement", statement());
    }
}
