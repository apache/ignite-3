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

import java.util.Objects;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code ALTER TABLE } statement.
 */
public abstract class IgniteAbstractSqlAlterTable extends SqlDdl {
    /** SqlNode identifier name. */
    protected final SqlIdentifier name;

    /** Constructor. */
    protected IgniteAbstractSqlAlterTable(IgniteDdlOperator operator, SqlParserPos pos, SqlIdentifier tblName) {
        super(operator, pos);
        name = Objects.requireNonNull(tblName, "table name");
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDdlOperator getOperator() {
        return (IgniteDdlOperator) super.getOperator();
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());

        if (ifExists()) {
            writer.keyword("IF EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        unparseAlterTableOperation(writer, leftPrec, rightPrec);
    }

    /**
     * Unparse rest of the ALTER TABLE command.
     */
    protected abstract void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec);

    /**
     * Ident name.
     *
     * @return Name of the object.
     */
    public SqlIdentifier name() {
        return name;
    }

    /**
     * Exist flag.
     *
     * @return Whether the IF EXISTS is specified.
     */
    public boolean ifExists() {
        IgniteDdlOperator operator = getOperator();
        return operator.existFlag();
    }
}
