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
import org.jetbrains.annotations.Nullable;

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
                    return new IgniteSqlExplain(pos, operands[0], operands[1], 0);
                }
            };

    private SqlNode explicandum;
    private SqlNode mode;
    private final int dynamicParameterCount;

    /** Constructor. */
    public IgniteSqlExplain(SqlParserPos pos,
            SqlNode explicandum,
            SqlNode mode,
            int dynamicParameterCount) {
        super(pos);
        this.explicandum = explicandum;
        this.mode = mode;
        this.dynamicParameterCount = dynamicParameterCount;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(explicandum, mode);
    }

    /**
     * Returns the underlying SQL statement to be explained.
     */
    public SqlNode getExplicandum() {
        return explicandum;
    }

    /**
     * Returns explain mode.
     */
    public SqlNode getMode() {
        return mode;
    }

    /**
     * Returns the number of dynamic parameters in the statement.
     */
    public int getDynamicParamCount() {
        return dynamicParameterCount;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXPLAIN");
        mode.unparse(writer, 0, 0);
        writer.keyword("FOR");
        writer.newlineAndIndent();
        explicandum.unparse(writer, leftPrec, rightPrec);
    }
}
