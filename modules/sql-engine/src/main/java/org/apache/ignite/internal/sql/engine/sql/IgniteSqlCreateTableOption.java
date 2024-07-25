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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An AST node representing option to create table with. */
public class IgniteSqlCreateTableOption extends SqlCall {

    /** Table option. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        /** Constructor. */
        protected Operator() {
            super("TableOption", SqlKind.OTHER);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlCreateTableOption((SqlIdentifier) operands[0], operands[1], pos);
        }
    }

    private static final SqlOperator OPERATOR = new Operator();

    /** Option key. */
    private final SqlIdentifier key;

    /** Option value. */
    private final SqlNode value;

    /** Creates IgniteSqlCreateTableOption. */
    public IgniteSqlCreateTableOption(SqlIdentifier key, SqlNode value, SqlParserPos pos) {
        super(pos);

        this.key = key;
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return List.of(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlCreateTableOption(key, value, pos);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlCreateTableOption)) {
            return litmus.fail("{} != {}", this, node);
        }

        IgniteSqlCreateTableOption that = (IgniteSqlCreateTableOption) node;
        if (key != that.key) {
            return litmus.fail("{} != {}", this, node);
        }

        return value.equalsDeep(that.value, litmus);
    }

    /**
     * Get option's key.
     */
    public SqlIdentifier key() {
        return key;
    }

    /**
     * Get option's value.
     */
    public SqlNode value() {
        return value;
    }
}
