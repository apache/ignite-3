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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN} statement.
 */
public class IgniteSqlAlterColumn extends IgniteAbstractSqlAlterTable {

    /** ALTER TABLE .. ALTER COLUMN operator. */
    protected static class Operator extends IgniteDdlOperator {

        private final boolean dropDefault;

        private final Boolean notNull;

        /** Constructor. */
        protected Operator(boolean ifExists, boolean dropDefault, Boolean notNull) {
            super("ALTER TABLE", SqlKind.ALTER_TABLE, ifExists);
            this.dropDefault = dropDefault;
            this.notNull = notNull;
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlAlterColumn(pos, existFlag(), (SqlIdentifier) operands[0], (SqlIdentifier) operands[1],
                    (SqlDataTypeSpec) operands[2], dropDefault, operands[3], notNull);
        }
    }

    private final SqlIdentifier columnName;
    private final SqlDataTypeSpec type;
    private final boolean dropDefault;
    private final SqlNode dflt;
    private final Boolean notNull;

    /** Constructor. */
    public IgniteSqlAlterColumn(
            SqlParserPos pos,
            boolean ifExists,
            SqlIdentifier tblName,
            SqlIdentifier columnName,
            @Nullable SqlDataTypeSpec type,
            boolean dropDefault,
            @Nullable SqlNode dflt,
            @Nullable Boolean notNull
    ) {
        super(new Operator(ifExists, dropDefault, notNull), pos, tblName);

        this.columnName = columnName;
        this.type = type;
        this.dropDefault = dropDefault;
        this.dflt = dflt;
        this.notNull = notNull;
    }

    /** Gets column name. */
    public SqlIdentifier columnName() {
        return columnName;
    }

    /**
     * Gets column data type specification.
     *
     * @return Column data type specification, {@code null} if the type does not need to be changed.
     */
    public @Nullable SqlDataTypeSpec dataType() {
        return type;
    }

    /**
     * Gets the new column DEFAULT expression.
     *
     * @return DEFAULT expression or {@code null} if the DEFAULT value does not need to be changed.
     */
    public @Nullable SqlNode expression() {
        return dflt;
    }

    /**
     * Gets the {@code NOT NULL} constraint change flag.
     *
     * @return {@code True} if the constraint should be added, @code false} if the constraint should be removed,{@code null} if this flag
     *         does not need to be changed.
     */
    public @Nullable Boolean notNull() {
        return notNull;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnName, type, dflt);
    }

    /** {@inheritDoc} */
    @Override
    protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        writer.keyword("COLUMN");

        columnName().unparse(writer, leftPrec, rightPrec);

        if (type != null) {
            writer.keyword("SET DATA TYPE");

            type.unparse(writer, 0, 0);

            if (notNull != null) {
                if (notNull) {
                    writer.keyword("NOT");
                }

                writer.keyword("NULL");
            }

            if (dflt != null) {
                writer.keyword("DEFAULT");

                dflt.unparse(writer, leftPrec, rightPrec);
            }

            return;
        }

        if (notNull != null) {
            writer.keyword(notNull ? "SET" : "DROP");
            writer.keyword("NOT NULL");
        }

        if (dropDefault) {
            writer.keyword("DROP DEFAULT");
        } else if (dflt != null) {
            writer.keyword("SET DEFAULT");

            dflt.unparse(writer, leftPrec, rightPrec);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        for (SqlNode node : getOperandList()) {
            if (node == null) {
                continue;
            }
            node.validate(validator, scope);
        }
    }
}
