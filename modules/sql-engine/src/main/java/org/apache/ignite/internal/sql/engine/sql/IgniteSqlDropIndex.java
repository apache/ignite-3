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
import org.apache.ignite.internal.sql.engine.exec.fsm.DdlBatchAware;
import org.apache.ignite.internal.sql.engine.exec.fsm.DdlBatchGroup;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code DROP INDEX} statement.
 */
// Note: This operation changes index state for available indexes and can't be batched with DROP TABLE operations.
//    Actually, this is a dirty hack and it can and should be batched, but "the voices in one's had" do NOT allow just ignore DROP_INDEX
//    late event and the objective reality, when an index has been dropped together with it's table in the same DDL batch (script).
//    However, the same "voices" are ok with ignoring errors, when the table is dropped concurrently while the index purging is in progress.
@DdlBatchAware(group = DdlBatchGroup.DROP_INDEX)
public class IgniteSqlDropIndex extends SqlDrop {

    /** DROP INDEX operator. */
    protected static class Operator extends IgniteDdlOperator {

        /** Constructor. */
        protected Operator(boolean existFlag) {
            super("DROP INDEX", SqlKind.DROP_INDEX, existFlag);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlDropIndex(pos, existFlag(), (SqlIdentifier) operands[0]);
        }
    }

    /** Index name. */
    private final SqlIdentifier indexName;

    /** Constructor. */
    public IgniteSqlDropIndex(SqlParserPos pos, boolean ifExists, SqlIdentifier idxName) {
        super(new Operator(ifExists), pos, ifExists);
        indexName = Objects.requireNonNull(idxName, "index name");
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDdlOperator getOperator() {
        return (IgniteDdlOperator) super.getOperator();
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(indexName);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName()); // "DROP ..."

        if (ifExists()) {
            writer.keyword("IF EXISTS");
        }

        indexName.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier indexName() {
        return indexName;
    }

    public boolean ifExists() {
        Operator operator = (Operator) getOperator();
        return operator.existFlag();
    }
}
