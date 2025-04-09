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

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.Nullable;

/**
 * Creates a SqlColumnDeclaration. Should be used instead of {@link SqlDdlNodes#column} because it supports cloning.
 */
public class IgniteSqlColumnDeclaration extends SqlCall {
    /** Column declaration operator. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        private final ColumnStrategy columnStrategy;

        /** Constructor. */
        protected Operator(ColumnStrategy columnStrategy) {
            super("COLUMN_DECL", SqlKind.COLUMN_DECL);
            this.columnStrategy = columnStrategy;
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral name, SqlParserPos pos, @Nullable SqlNode... operands) {
            SqlIdentifier id = (SqlIdentifier) operands[0];
            SqlDataTypeSpec dataType = (SqlDataTypeSpec) operands[1];
            SqlNode expression = operands.length > 2 ? operands[2] : null;

            return new IgniteSqlColumnDeclaration(pos, id, dataType, expression, columnStrategy);
        }
    }

    private final Operator operator;
    private final SqlIdentifier name;
    private final SqlDataTypeSpec dataType;
    private final @Nullable SqlNode expression;
    private final ColumnStrategy strategy;

    /** Constructor */
    public IgniteSqlColumnDeclaration(SqlParserPos pos, SqlIdentifier name,
            SqlDataTypeSpec dataType, @Nullable SqlNode expression,
            ColumnStrategy strategy) {
        super(pos);

        this.operator = new Operator(strategy);
        this.name = name;
        this.dataType = dataType;
        this.expression = expression;
        this.strategy = strategy;
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name, dataType, expression);
    }

    public SqlIdentifier name() {
        return name;
    }

    public SqlDataTypeSpec dataType() {
        return dataType;
    }

    public @Nullable SqlNode expression() {
        return expression;
    }

    public ColumnStrategy strategy() {
        return strategy;
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, 0, 0);
        dataType.unparse(writer, 0, 0);
        if (Boolean.FALSE.equals(dataType.getNullable())) {
            writer.keyword("NOT NULL");
        }
        SqlNode expression = this.expression;
        if (expression != null) {
            switch (strategy) {
                case VIRTUAL:
                case STORED:
                    writer.keyword("AS");
                    exp(writer, expression);
                    writer.keyword(strategy.name());
                    break;
                case DEFAULT:
                    writer.keyword("DEFAULT");
                    exp(writer, expression);
                    break;
                default:
                    throw new AssertionError("unexpected: " + strategy);
            }
        }
    }

    private static void exp(SqlWriter writer, SqlNode expression) {
        if (writer.isAlwaysUseParentheses()) {
            expression.unparse(writer, 0, 0);
        } else {
            writer.sep("(");
            expression.unparse(writer, 0, 0);
            writer.sep(")");
        }
    }
}
