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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Base class for SQL constraints. */
public abstract class IgniteSqlKeyConstraint extends SqlCall {

    protected final @Nullable SqlIdentifier name;

    protected final SqlNodeList columnList;

    protected final SqlOperator operator;

    /** Constructor. */
    public IgniteSqlKeyConstraint(SqlOperator operator,
            SqlParserPos pos,
            @Nullable SqlIdentifier name,
            SqlNodeList columnList
    ) {
        super(pos);

        this.name = name;
        this.operator = Objects.requireNonNull(operator, "operator");
        this.columnList = Objects.requireNonNull(columnList, "columnList");
    }

    /** Constraint name. */
    public @Nullable SqlIdentifier getName() {
        return name;
    }

    /** List of columns. */
    public SqlNodeList getColumnList() {
        return columnList;
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    /** {@inheritDoc} */
    @Override
    public abstract void unparse(SqlWriter writer, int leftPrec, int rightPrec);
}
