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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Parse tree for {@code DROP SCHEMA} statement.
 */
public class IgniteSqlDropSchema extends SqlDrop {

    /** DROP SCHEMA operator. */
    protected static class Operator extends IgniteDdlOperator {
        private final IgniteSqlDropSchemaBehavior dropBehavior;

        /** Constructor. */
        protected Operator(boolean existFlag, IgniteSqlDropSchemaBehavior dropBehavior) {
            super("DROP SCHEMA", SqlKind.OTHER_DDL, existFlag);

            this.dropBehavior = dropBehavior;
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos,
                @Nullable SqlNode... operands) {
            return new IgniteSqlDropSchema(pos, existFlag(), (SqlIdentifier) operands[0], dropBehavior);
        }
    }

    /** Schema name. */
    private final SqlIdentifier name;

    /** Drop behavior. */
    private final IgniteSqlDropSchemaBehavior behavior;

    /** Constructor. */
    public IgniteSqlDropSchema(SqlParserPos pos, boolean ifExists, SqlIdentifier name, IgniteSqlDropSchemaBehavior behavior) {
        super(new Operator(ifExists, behavior), pos, ifExists);

        this.name = Objects.requireNonNull(name, "schema name");
        this.behavior = behavior;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDdlOperator getOperator() {
        return (IgniteDdlOperator) super.getOperator();
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName()); // "DROP ..."

        if (ifExists) {
            writer.keyword("IF EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        switch (behavior) {
            case RESTRICT:
                writer.keyword("RESTRICT");
                break;
            case CASCADE:
                writer.keyword("CASCADE");
                break;
            case IMPLICIT_RESTRICT:
                break;
            default:
                throw new IllegalStateException("Unexpected drop behavior: " + behavior);
        }
    }

    public SqlIdentifier name() {
        return name;
    }

    public boolean ifExists() {
        Operator operator = (Operator) getOperator();
        return operator.existFlag();
    }

    public IgniteSqlDropSchemaBehavior behavior() {
        return behavior;
    }
}
