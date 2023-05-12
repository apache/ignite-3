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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN} statement.
 */
public abstract class IgniteSqlAlterColumn extends IgniteAbstractSqlAlterTable {
    /** Constructor. */
    IgniteSqlAlterColumn(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName) {
        super(pos, ifExists, tblName);
    }

    /**
     * Gets column name.
     *
     * @return Column name.
     */
    public abstract SqlIdentifier columnName();

    /** {@inheritDoc} */
    @Override protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        writer.keyword("COLUMN");

        columnName().unparse(writer, leftPrec, rightPrec);

        unparseAlterColumnOperation(writer, leftPrec, rightPrec);
    }

    /**
     * Unparse rest of the ALTER TABLE ... ALTER COLUMN command.
     */
    protected abstract void unparseAlterColumnOperation(SqlWriter writer, int leftPrec, int rightPrec);
}
