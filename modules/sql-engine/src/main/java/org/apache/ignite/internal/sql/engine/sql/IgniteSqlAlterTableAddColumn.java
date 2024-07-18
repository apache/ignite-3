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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Parse tree for {@code ALTER TABLE ... ADD COLUMN} statement.
 */
public class IgniteSqlAlterTableAddColumn extends IgniteAbstractSqlAlterTable {

    /** ALTER TABLE ... ADD COLUMN operator. */
    protected static class Operator extends IgniteDdlOperator {

        /** Constructor. */
        protected Operator(boolean existFlag) {
            super("ALTER TABLE", SqlKind.ALTER_TABLE, existFlag);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlAlterTableAddColumn(pos, existFlag(), (SqlIdentifier) operands[0], (SqlNodeList) operands[1]);
        }
    }

    /** Introduced columns. */
    private final SqlNodeList columns;

    /** Constructor. */
    public IgniteSqlAlterTableAddColumn(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName,
            SqlNodeList columns) {
        super(new Operator(ifExists), pos, tblName);
        this.columns = Objects.requireNonNull(columns, "columns list");
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns);
    }

    /** {@inheritDoc} */
    @Override
    protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ADD");
        writer.keyword("COLUMN");

        columns.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        name.validate(validator, scope);

        for (SqlNode column : columns) {
            // There is no call to validate SqlColumnDeclaration on SqlValidator
            if (column instanceof SqlColumnDeclaration) {
                SqlColumnDeclaration col = (SqlColumnDeclaration) column;
                col.name.validate(validator, scope);
                col.dataType.validate(validator, scope);
                // we do not validate column expression
            }
        }

        columns.validate(validator, scope);
    }

    /** Processing columns definition. */
    public SqlNodeList columns() {
        return columns;
    }
}
