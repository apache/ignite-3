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
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN} statement.
 */
public class IgniteSqlAlterTableAlterColumn extends IgniteAbstractSqlAlterTable {
    /** Introduced columns. */
    private final SqlColumnDeclaration column;

    /** Constructor. */
    public IgniteSqlAlterTableAlterColumn(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName, SqlNode column) {
        super(pos, ifExists, tblName);

        this.column = (SqlColumnDeclaration) Objects.requireNonNull(column, "column");
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, column);
    }

    public SqlIdentifier columnName() {
        return column.name;
    }

    public @Nullable SqlNode defaultExpression() {
        return column.expression;
    }

    public @Nullable SqlDataTypeSpec dataType() {
        return column.dataType;
    }

    public ColumnStrategy strategy() {
        return column.strategy;
    }

    public boolean addNotNull() {
        return column.dataType == null
                && column.strategy != null
                && column.strategy == ColumnStrategy.NOT_NULLABLE;
    }

    public boolean dropNotNull() {
        return column.dataType == null
                && column.strategy != null
                && column.strategy == ColumnStrategy.NULLABLE;
    }

    public boolean addDefault() {
        return column.dataType == null
                && column.strategy != null
                && column.strategy == ColumnStrategy.DEFAULT
                && column.expression != null;
    }

    public boolean dropDefault() {
        return column.dataType == null
                && column.strategy != null
                && column.strategy == ColumnStrategy.DEFAULT
                && column.expression == null;
    }

    /** {@inheritDoc} */
    @Override protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        writer.keyword("COLUMN");

        column.name.unparse(writer, leftPrec, rightPrec);

        if (column.dataType != null) {
            writer.keyword("SET DATA TYPE");

            column.dataType.unparse(writer, 0, 0);

            if (Boolean.FALSE.equals(column.dataType.getNullable())) {
                writer.keyword("NOT NULL");
            }

            SqlNode expression = column.expression;

            if (expression != null) {
                if (column.strategy == ColumnStrategy.DEFAULT) {
                    writer.keyword("DEFAULT");

                    column.expression.unparse(writer, 0, 0);
                } else {
                    throw new AssertionError("Unexpected strategy: " + column.strategy);
                }
            }
        } else {
            switch (column.strategy) {
                case DEFAULT:
                    if (column.expression != null) {
                        writer.keyword("SET DEFAULT");

                        column.expression.unparse(writer, 0, 0);
                    } else {
                        writer.keyword("DROP DEFAULT");
                    }

                    break;
                case NULLABLE:
                    writer.keyword("DROP NOT NULL");

                    break;
                case NOT_NULLABLE:
                    writer.keyword("SET NOT NULL");

                    break;
                default:
                    throw new AssertionError("Unexpected strategy: " + column.strategy);
            }
        }
    }
}
