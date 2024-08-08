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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code UPDATE} statement. Fixes incomplete operator of original {@link SqlUpdate}.
 */
public class IgniteSqlUpdate extends SqlUpdate {

    /** UPDATE operator. */
    protected static final class Operator extends SqlSpecialOperator {

        /** Constructor. */
        protected Operator() {
            super("UPDATE", SqlKind.UPDATE);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlUpdate(pos, operands[0], (SqlNodeList) operands[1],
                    (SqlNodeList) operands[2], operands[3],
                    (SqlSelect) operands[4], (SqlIdentifier) operands[5]);
        }
    }

    private static final Operator OPERATOR = new Operator();

    /** Constructor. */
    public IgniteSqlUpdate(SqlParserPos pos, SqlNode targetTable,
            SqlNodeList targetColumnList, SqlNodeList sourceExpressionList,
            @Nullable SqlNode condition,
            @Nullable SqlSelect sourceSelect,
            @Nullable SqlIdentifier alias) {
        super(pos, targetTable, targetColumnList, sourceExpressionList, condition, sourceSelect, alias);
    }

    /** Constructor.*/
    public IgniteSqlUpdate(SqlUpdate node) {
        this(node.getParserPosition(), node.getTargetTable(), node.getTargetColumnList(),
                node.getSourceExpressionList(), node.getCondition(),
                node.getSourceSelect(), node.getAlias());
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("nullness")
    @Override
    public List<@Nullable SqlNode> getOperandList() {
        return ImmutableNullableList.of(getTargetTable(), getTargetColumnList(), getSourceExpressionList(),
                getCondition(), getSourceSelect(), getAlias());
    }
}
