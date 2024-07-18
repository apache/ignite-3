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
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code CREATE TABLE} statement with Ignite specific features.
 */
public class IgniteSqlCreateTable extends SqlCreate {

    /** CREATE TABLE operator. */
    protected static class Operator extends IgniteDdlOperator {

        /** Constructor. */
        protected Operator(boolean existFlag) {
            super("CREATE TABLE", SqlKind.CREATE_TABLE, existFlag);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos,
                @Nullable SqlNode... operands) {
            return new IgniteSqlCreateTable(pos, existFlag(), (SqlIdentifier) operands[0], (SqlNodeList) operands[1],
                    (SqlNodeList) operands[2], (SqlNodeList) operands[3]);
        }
    }

    private final SqlIdentifier name;

    private final @Nullable SqlNodeList columnList;

    private final @Nullable SqlNodeList colocationColumns;

    private final @Nullable SqlNodeList createOptionList;

    /** Creates a SqlCreateTable. */
    public IgniteSqlCreateTable(
            SqlParserPos pos,
            boolean ifNotExists,
            SqlIdentifier name,
            @Nullable SqlNodeList columnList,
            @Nullable SqlNodeList colocationColumns,
            @Nullable SqlNodeList createOptionList
    ) {
        super(new Operator(ifNotExists), pos, false, ifNotExists);

        this.name = Objects.requireNonNull(name, "name");
        this.columnList = columnList;
        this.colocationColumns = colocationColumns;
        this.createOptionList = createOptionList;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDdlOperator getOperator() {
        return (IgniteDdlOperator) super.getOperator();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("nullness")
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, colocationColumns, createOptionList);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (ifNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }

        if (colocationColumns != null) {
            writer.keyword("COLOCATE BY");

            SqlWriter.Frame frame = writer.startList("(", ")");
            colocationColumns.unparse(writer, 0, 0);
            writer.endList(frame);
        }

        if (createOptionList != null) {
            writer.keyword("WITH");

            createOptionList.unparse(writer, 0, 0);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        for (SqlNode node : getOperandList()) {
            if (node == null) {
                continue;
            }
            node.validate(validator, scope);
        }
    }

    /**
     * Get name of the table.
     */
    public SqlIdentifier name() {
        return name;
    }

    /**
     * Get list of the specified columns and constraints.
     */
    public SqlNodeList columnList() {
        return columnList;
    }

    /**
     * Get list of the colocation columns.
     */
    public SqlNodeList colocationColumns() {
        return colocationColumns;
    }

    /**
     * Get list of the specified options to create table with.
     */
    public SqlNodeList createOptionList() {
        return createOptionList;
    }

    /**
     * Get whether the IF NOT EXISTS is specified.
     */
    public boolean ifNotExists() {
        Operator operator = (Operator) getOperator();
        return operator.existFlag();
    }
}
