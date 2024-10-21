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
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code KILL ... } statement.
 */
public class IgniteSqlKill extends SqlCall {

    /** KILL operator. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        private final IgniteSqlKillObjectType objectType;

        private final SqlLiteral objectId;

        private final boolean waitForCompletion;

        /** Constructor. */
        protected Operator(
                IgniteSqlKillObjectType objectType,
                SqlLiteral objectId,
                boolean waitForCompletion
        ) {
            super("KILL", SqlKind.OTHER);

            this.objectType = Objects.requireNonNull(objectType, "objectType");
            this.objectId = Objects.requireNonNull(objectId, "objectId");
            this.waitForCompletion = waitForCompletion;
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos,
                @Nullable SqlNode... operands) {

            return new IgniteSqlKill(pos, objectType, (SqlLiteral) operands[0], waitForCompletion);
        }
    }

    private final Operator operator;

    /** Constructor. */
    public IgniteSqlKill(
            SqlParserPos pos,
            IgniteSqlKillObjectType objectType,
            SqlLiteral objectId,
            boolean waitForCompletion
    ) {
        super(pos);

        this.operator = new Operator(objectType, objectId, waitForCompletion);
    }

    /** Object id. */
    public SqlLiteral objectId() {
        return operator.objectId;
    }

    /** Type of object. */
    public IgniteSqlKillObjectType objectType() {
        return operator.objectType;
    }

    /** Wait for completion or not. */
    public boolean waitForCompletion() {
        return operator.waitForCompletion;
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return List.of(operator.objectId);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        switch (operator.objectType) {
            case QUERY:
                writer.keyword("QUERY");
                break;
            case TRANSACTION:
                writer.keyword("TRANSACTION");
                break;
            case COMPUTE:
                writer.keyword("COMPUTE");
                break;
            default:
                throw new IllegalStateException("Unexpected object type: " + operator.objectType);
        }

        objectId().unparse(writer, 0, 0);

        if (!operator.waitForCompletion) {
            writer.keyword("NO");
            writer.keyword("WAIT");
        }
    }
}
