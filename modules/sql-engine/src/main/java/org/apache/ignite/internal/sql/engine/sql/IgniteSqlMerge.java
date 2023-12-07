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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code MERGE} statement, Fixes incomplete operator of original {@link SqlMerge}.
 */
public class IgniteSqlMerge extends SqlMerge {

    /** MERGE operator. */
    protected static class Operator extends SqlSpecialOperator {

        /** Constructor. */
        protected Operator() {
            super("MERGE", SqlKind.MERGE);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlMerge(pos, operands[0], operands[1],
                    operands[2], (SqlUpdate) operands[3],
                    (SqlInsert) operands[4], (SqlSelect) operands[5],
                    (SqlIdentifier) operands[6]);
        }
    }

    private static final Operator OPERATOR = new Operator();

    /** Constructor. */
    public IgniteSqlMerge(SqlParserPos pos, SqlNode targetTable,
            SqlNode condition, SqlNode source,
            @Nullable SqlUpdate updateCall,
            @Nullable SqlInsert insertCall,
            @Nullable SqlSelect sourceSelect,
            @Nullable SqlIdentifier alias) {
        super(pos, targetTable, condition, source, updateCall, insertCall, sourceSelect, alias);
    }

    /** Constructor. */
    public IgniteSqlMerge(SqlMerge node) {
        this(node.getParserPosition(), node.getTargetTable(), node.getCondition(),
                node.getSourceTableRef(), node.getUpdateCall(), node.getInsertCall(),
                node.getSourceSelect(), node.getAlias()
        );
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }
}
