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
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code DELETE} statement. Fixes incomplete operator of original {@link SqlDelete}.
 */
public class IgniteSqlDelete extends SqlDelete {

    /** DELETE operator. */
    private static final class Operator extends SqlSpecialOperator {

        private Operator() {
            super("DELETE", SqlKind.DELETE);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlDelete(pos, operands[0], operands[1], (SqlSelect) operands[2], (SqlIdentifier) operands[3]);
        }
    }

    private static final SqlOperator OPERATOR = new Operator();

    /** Constructor. */
    public IgniteSqlDelete(SqlParserPos pos, SqlNode targetTable,
            @Nullable SqlNode condition,
            @Nullable SqlSelect sourceSelect,
            @Nullable SqlIdentifier alias) {
        super(pos, targetTable, condition, sourceSelect, alias);
    }

    /** Constructor. */
    public IgniteSqlDelete(SqlDelete node) {
        this(node.getParserPosition(), node.getTargetTable(), node.getCondition(), node.getSourceSelect(), node.getAlias());
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getTargetTable(), getCondition(), getSourceSelect(), getAlias());
    }

}
