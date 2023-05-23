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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN} statement.
 */
public class IgniteSqlAlterColumn extends IgniteAbstractSqlAlterTable {
    private final SqlNodeList actions;
    private final SqlIdentifier columnName;

    /** Constructor. */
    public IgniteSqlAlterColumn(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName, SqlIdentifier columnName, SqlNodeList actions) {
        super(pos, ifExists, tblName);

        this.columnName = columnName;
        this.actions = actions;
    }

    /** Gets column name. */
    public SqlIdentifier columnName() {
        return columnName;
    }

    /** ets alter column actions list. */
    public List<SqlNode> actions() {
        return actions;
    }

    /** {@inheritDoc} */
    @Override protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        writer.keyword("COLUMN");

        columnName().unparse(writer, leftPrec, rightPrec);

        actions.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnName, actions);
    }
}
