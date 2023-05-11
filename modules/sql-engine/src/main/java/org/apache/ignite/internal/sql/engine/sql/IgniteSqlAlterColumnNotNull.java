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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN ... SET/DROP NOT NULL} statement.
 */
public class IgniteSqlAlterColumnNotNull extends IgniteSqlAlterColumn {
    /** Column name. */
    private final SqlIdentifier columnName;

    /** NOT NULL constraint change flag. */
    private final boolean notNull;

    /** Constructor. */
    public IgniteSqlAlterColumnNotNull(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName, SqlIdentifier colName, boolean notNull) {
        super(pos, ifExists, tblName);

        this.columnName = Objects.requireNonNull(colName, "columnName");
        this.notNull = notNull;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnName);
    }

    /** {@inheritDoc} */
    @Override public SqlIdentifier columnName() {
        return columnName;
    }

    /**
     * Gets the {@code NOT NULL} constraint change flag.
     *
     * @return {@code True} if the constraint should be added, @code false} if the constraint should be removed.
     */
    public boolean notNull() {
        return notNull;
    }

    /** {@inheritDoc} */
    @Override protected void unparseAlterColumnOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(notNull ? "SET" : "DROP");
        writer.keyword("NOT NULL");
    }
}
