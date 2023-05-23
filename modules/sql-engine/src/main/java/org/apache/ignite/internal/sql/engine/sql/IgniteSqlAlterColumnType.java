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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE} statement.
 */
public class IgniteSqlAlterColumnType extends IgniteSqlAlterColumn {
    /** Column declaration. */
    private final SqlDataTypeSpec type;

    private final SqlIdentifier colName;

    /** Constructor. */
    public IgniteSqlAlterColumnType(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName, SqlIdentifier colName, SqlDataTypeSpec type) {
        super(pos, ifExists, tblName);

        this.colName = Objects.requireNonNull(colName, "column name");
        this.type = Objects.requireNonNull(type, "type");
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, colName, type);
    }

    /** {@inheritDoc} */
    @Override public SqlIdentifier columnName() {
        return colName;
    }

    /**
     * Gets column data type specification.
     *
     * @return Column data type specification.
     */
    public SqlDataTypeSpec dataType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override protected void unparseAlterColumnOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SET DATA TYPE");

        type.unparse(writer, 0, 0);
    }
}
