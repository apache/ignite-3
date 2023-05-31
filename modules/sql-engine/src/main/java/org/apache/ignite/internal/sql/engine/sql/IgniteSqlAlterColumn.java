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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN} statement.
 */
public class IgniteSqlAlterColumn extends IgniteAbstractSqlAlterTable {
    private final SqlIdentifier columnName;
    private final SqlDataTypeSpec type;
    private final SqlNode dflt;
    private final Boolean notNull;

    /** Constructor. */
    public IgniteSqlAlterColumn(
            SqlParserPos pos,
            boolean ifExists,
            SqlIdentifier tblName,
            SqlIdentifier columnName,
            SqlDataTypeSpec type,
            SqlNode dflt,
            Boolean notNull
    ) {
        super(pos, ifExists, tblName);

        this.columnName = columnName;
        this.type = type;
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
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnName, type, dflt);
    }

    /** {@inheritDoc} */
    @Override protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        writer.keyword("COLUMN");

        columnName().unparse(writer, leftPrec, rightPrec);

        if (type != null) {
            writer.keyword("SET DATA TYPE");

            type.unparse(writer, 0, 0);

            if (notNull != null) {
                if (notNull) {
                    writer.keyword("NOT NULL");
                } else {
                    writer.keyword("NULLABLE");
                }
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

        if (dflt != null) {
            if (dflt instanceof SqlLiteral && ((SqlLiteral) dflt).getTypeName() == SqlTypeName.NULL) {
                writer.keyword("DROP DEFAULT");
            } else {
                writer.keyword("SET DEFAULT");

                dflt.unparse(writer, leftPrec, rightPrec);
            }
        }
    }
}
