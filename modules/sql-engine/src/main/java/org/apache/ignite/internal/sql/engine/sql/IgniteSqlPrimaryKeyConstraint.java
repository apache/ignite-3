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
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Primary key constraint. */
public class IgniteSqlPrimaryKeyConstraint extends IgniteSqlKeyConstraint {

    /** Primary key constraint. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        private final IgniteSqlPrimaryKeyIndexType indexType;

        /** Constructor. */
        protected Operator(IgniteSqlPrimaryKeyIndexType indexType) {
            super("PRIMARY KEY", SqlKind.PRIMARY_KEY);
            this.indexType = Objects.requireNonNull(indexType, "indexType");
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlPrimaryKeyConstraint(pos,
                    (SqlIdentifier) operands[0],
                    new SqlNodeList(Arrays.asList(operands), pos),
                    indexType
            );
        }
    }

    /** Constructor. */
    public IgniteSqlPrimaryKeyConstraint(SqlParserPos pos,
            @Nullable SqlIdentifier name,
            SqlNodeList columnList,
            IgniteSqlPrimaryKeyIndexType indexType
    ) {
        super(new Operator(indexType), pos, name, columnList);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList);
    }

    /** Type of this primary key index. */
    public IgniteSqlPrimaryKeyIndexType getIndexType() {
        return operator().indexType;
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (name != null) {
            writer.keyword("CONSTRAINT");
            name.unparse(writer, 0, 0);
        }
        writer.keyword(getOperator().getName());

        Operator operator = operator();
        if (operator.indexType != IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH) {
            writer.keyword("USING");

            writer.keyword(operator.indexType.name());
        }

        columnList.unparse(writer, 1, 1);
    }

    /** {@inheritDoc} */
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (name != null) {
            name.validate(validator, scope);
        }

        columnList.validate(validator, scope);
    }

    protected Operator operator() {
        return (Operator) getOperator();
    }
}
