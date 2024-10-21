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

        private final @Nullable Boolean noWait;

        /** Constructor. */
        protected Operator(
                IgniteSqlKillObjectType objectType,
                SqlLiteral objectId,
                @Nullable Boolean noWait
        ) {
            super("KILL", SqlKind.OTHER);

            this.objectType = Objects.requireNonNull(objectType, "objectType");
            this.objectId = Objects.requireNonNull(objectId, "objectId");
            this.noWait = noWait;
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos,
                @Nullable SqlNode... operands) {

            return new IgniteSqlKill(pos, objectType, (SqlLiteral) operands[0], noWait);
        }
    }

    private final Operator operator;

    /** Constructor. */
    public IgniteSqlKill(
            SqlParserPos pos,
            IgniteSqlKillObjectType objectType,
            SqlLiteral objectId,
            @Nullable Boolean noWait
    ) {
        super(pos);

        this.operator = new Operator(objectType, objectId, noWait);
    }

    /** Object id. */
    public SqlLiteral objectId() {
        return operator.objectId;
    }

    /** Type of object. */
    public IgniteSqlKillObjectType objectType() {
        return operator.objectType;
    }

    /** Do not wait for completion if {@code true}. */
    public @Nullable Boolean noWait() {
        return operator.noWait;
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
        writer.keyword(objectType().name());

        objectId().unparse(writer, leftPrec, rightPrec);

        if (operator.noWait != null && operator.noWait) {
            writer.keyword("NO");
            writer.keyword("WAIT");
        }
    }
}
