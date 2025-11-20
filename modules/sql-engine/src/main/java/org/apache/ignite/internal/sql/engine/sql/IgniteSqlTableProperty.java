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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.jetbrains.annotations.Nullable;

/** An AST node representing table property in CREATE TABLE and ALTER TABLE statements. */
public class IgniteSqlTableProperty extends SqlCall {
    /** TABLE property operator. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        /** Constructor. */
        protected Operator() {
            super("TableProperty", SqlKind.OTHER);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlTableProperty((SqlLiteral) operands[0], operands[1], pos);
        }
    }

    private static final SqlOperator OPERATOR = new Operator();

    private final SqlLiteral key;
    private final SqlNode value;

    /** Creates {@link IgniteSqlTableProperty}. */
    public IgniteSqlTableProperty(SqlLiteral key, SqlNode value, SqlParserPos pos) {
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
        return new IgniteSqlTableProperty(key, value, pos);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(key().sqlName);

        value.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlTableProperty)) {
            return litmus.fail("{} != {}", this, node);
        }

        IgniteSqlTableProperty that = (IgniteSqlTableProperty) node;
        if (key != that.key) {
            return litmus.fail("{} != {}", this, node);
        }

        return value.equalsDeep(that.value, litmus);
    }

    /**
     * Get property's key.
     */
    public IgniteSqlTablePropertyKey key() {
        return key.getValueAs(IgniteSqlTablePropertyKey.class);
    }

    /**
     * Get property's value.
     */
    public SqlNode value() {
        return value;
    }
}
