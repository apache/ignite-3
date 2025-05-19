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

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>IgniteSqlExplain</code> is a node of a parse tree which represents an EXPLAIN statement.
 */
public class IgniteSqlExplain extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("EXPLAIN", SqlKind.EXPLAIN) {
                @SuppressWarnings("argument.type.incompatible")
                @Override
                public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                        SqlParserPos pos, @Nullable SqlNode... operands) {
                    return new IgniteSqlExplain(pos, operands[0], 0);
                }
            };


    private SqlNode explicandum;
    private final int dynamicParameterCount;

    /** Constructor. */
    public IgniteSqlExplain(SqlParserPos pos,
            SqlNode explicandum,
            int dynamicParameterCount) {
        super(pos);
        this.explicandum = explicandum;
        this.dynamicParameterCount = dynamicParameterCount;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(explicandum);
    }

    /**
     * Returns the underlying SQL statement to be explained.
     */
    public SqlNode getExplicandum() {
        return explicandum;
    }

    /**
     * Returns the number of dynamic parameters in the statement.
     */
    public int getDynamicParamCount() {
        return dynamicParameterCount;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXPLAIN PLAN FOR");
        writer.newlineAndIndent();
        explicandum.unparse(writer, leftPrec, rightPrec);
    }
}
