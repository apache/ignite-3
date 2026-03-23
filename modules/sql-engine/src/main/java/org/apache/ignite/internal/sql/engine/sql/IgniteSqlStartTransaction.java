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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code START TRANSACTION } statement.
 */
public class IgniteSqlStartTransaction extends SqlCall {

    /** START TRANSACTION operator. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        private final IgniteSqlStartTransactionMode mode;

        /** Constructor. */
        protected Operator(IgniteSqlStartTransactionMode mode) {
            super("START TRANSACTION", SqlKind.OTHER);
            this.mode = mode;
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlStartTransaction(pos, mode);
        }
    }

    private final Operator operator;

    /** Creates a IgniteSqlStartTransaction. */
    public IgniteSqlStartTransaction(SqlParserPos pos, IgniteSqlStartTransactionMode mode) {
        super(pos);

        this.operator = new Operator(mode);
    }

    /** Returns the transaction mode . */
    public IgniteSqlStartTransactionMode getMode() {
        return operator.mode;
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return List.of();
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());

        switch (operator.mode) {
            case READ_ONLY:
                writer.keyword("READ ONLY");
                break;
            case READ_WRITE:
                writer.keyword("READ WRITE");
                break;
            case IMPLICIT_READ_WRITE:
                break;
            default:
                throw new IllegalStateException("Unexpected start transaction mode: " + operator.mode);
        }
    }
}
