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
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.ignite.internal.sql.engine.exec.fsm.DdlBatchAware;
import org.apache.ignite.internal.sql.engine.exec.fsm.DdlBatchGroup;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Parse tree for {@code CREATE INDEX} statement.
 */
@DdlBatchAware(group = DdlBatchGroup.CREATE)
public class IgniteSqlCreateIndex extends SqlCreate {

    /** CREATE INDEX operator. */
    protected static class Operator extends IgniteDdlOperator {

        private final IgniteSqlIndexType indexType;

        /** Constructor. */
        protected Operator(IgniteSqlIndexType indexType, boolean existFlag) {
            super("CREATE INDEX", SqlKind.CREATE_INDEX, existFlag);
            this.indexType = indexType;
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlCreateIndex(pos, existFlag(), (SqlIdentifier) operands[0], (SqlIdentifier) operands[1],
                    indexType, (SqlNodeList) operands[2]);
        }
    }

    /** Idx name. */
    private final SqlIdentifier idxName;

    /** Table name. */
    private final SqlIdentifier tblName;

    private final IgniteSqlIndexType type;

    /** Columns involved. */
    private final SqlNodeList columnList;

    /** Creates a SqlCreateIndex. */
    public IgniteSqlCreateIndex(SqlParserPos pos, boolean ifNotExists, SqlIdentifier idxName, SqlIdentifier tblName,
            IgniteSqlIndexType type, SqlNodeList columnList) {
        super(new Operator(type, ifNotExists), pos, false, ifNotExists);
        this.idxName = Objects.requireNonNull(idxName, "index name");
        this.tblName = Objects.requireNonNull(tblName, "table name");
        this.type = Objects.requireNonNull(type, "type");
        this.columnList = columnList;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDdlOperator getOperator() {
        return (IgniteDdlOperator) super.getOperator();
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(idxName, tblName, columnList);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");

        writer.keyword("INDEX");

        if (ifNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }

        idxName.unparse(writer, 0, 0);

        writer.keyword("ON");

        tblName.unparse(writer, 0, 0);

        if (type != IgniteSqlIndexType.IMPLICIT_SORTED) {
            writer.keyword("USING");

            writer.keyword(type.name());
        }

        SqlWriter.Frame frame = writer.startList("(", ")");

        for (SqlNode c : columnList) {
            writer.sep(",");

            boolean desc = false;

            if (c.getKind() == SqlKind.DESCENDING) {
                c = ((SqlCall) c).getOperandList().get(0);
                desc = true;
            }

            c.unparse(writer, 0, 0);

            if (desc) {
                writer.keyword("DESC");
            }
        }

        writer.endList(frame);
    }

    public SqlIdentifier indexName() {
        return idxName;
    }

    public SqlIdentifier tableName() {
        return tblName;
    }

    public IgniteSqlIndexType type() {
        return type;
    }

    public SqlNodeList columnList() {
        return columnList;
    }

    public boolean ifNotExists() {
        Operator operator = (Operator) getOperator();
        return operator.existFlag();
    }
}
