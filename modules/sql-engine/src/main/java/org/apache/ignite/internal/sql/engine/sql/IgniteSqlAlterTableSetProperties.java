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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.ignite.internal.sql.engine.exec.fsm.DdlBatchAware;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Parse tree for {@code ALTER TABLE ... SET} statement.
 */
@DdlBatchAware
public class IgniteSqlAlterTableSetProperties extends IgniteAbstractSqlAlterTable {

    /** ALTER TABLE ... SET operator. */
    protected static class Operator extends IgniteDdlOperator {

        /** Constructor. */
        protected Operator(boolean existFlag) {
            super("ALTER TABLE", SqlKind.ALTER_TABLE, existFlag);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlAlterTableSetProperties(pos, existFlag(), (SqlIdentifier) operands[0], (SqlNodeList) operands[1]);
        }
    }

    private final SqlNodeList propertyList;

    /** Constructor. */
    public IgniteSqlAlterTableSetProperties(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName,
            SqlNodeList propertyList) {
        super(new Operator(ifExists), pos, tblName);
        this.propertyList = Objects.requireNonNull(propertyList, "property list");
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, propertyList);
    }

    /** {@inheritDoc} */
    @Override
    protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SET");

        SqlWriter.Frame frame = writer.startList("(", ")");
        propertyList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
    }

    /** Properties to set. */
    public SqlNodeList propertyList() {
        return propertyList;
    }
}
