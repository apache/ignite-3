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

import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A base class for a parse tree for {@code ALTER ZONE} statement.
 */
public abstract class IgniteAbstractSqlAlterZone extends SqlDdl {

    protected final SqlIdentifier name;

    /** Constructor. */
    protected IgniteAbstractSqlAlterZone(IgniteDdlOperator operator, SqlParserPos pos, SqlIdentifier name) {
        super(operator, pos);
        this.name = name;
    }

    /** The name of a distribution zone to alter. **/
    public SqlIdentifier name() {
        return name;
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

        unparseAlterZoneOperation(writer, leftPrec, rightPrec);
    }

    /**
     * Unparse rest of the ALTER ZONE command.
     */
    protected abstract void unparseAlterZoneOperation(SqlWriter writer, int leftPrec, int rightPrec);

    /** Returns whether IF EXISTS was specified or not. **/
    public boolean ifExists() {
        return getOperator().existFlag();
    }
}
