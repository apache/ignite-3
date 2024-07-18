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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class IgniteSqlDropTable extends SqlDrop {

    /** DROP TABLE operator. */
    protected static class Operator extends IgniteDdlOperator {

        /** Constructor. */
        public Operator(boolean existFlag) {
            super("DROP TABLE", SqlKind.DROP_TABLE, existFlag);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos,
                @Nullable SqlNode... operands) {
            return new IgniteSqlDropTable(pos, existFlag(), (SqlIdentifier) operands[0]);
        }
    }

    private final SqlIdentifier name;

    /** Constructor. */
    public IgniteSqlDropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
        super(new Operator(ifExists), pos, ifExists);

        this.name = name;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name);
    }

    /** Returns table name. */
    public SqlIdentifier name() {
        return name;
    }

    /** Whether "IF EXISTS" was specified. */
    public boolean ifExists() {
        IgniteDdlOperator operator = (IgniteDdlOperator) getOperator();
        return operator.existFlag();
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("TABLE");

        if (ifExists()) {
            writer.keyword("IF EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);
    }
}
