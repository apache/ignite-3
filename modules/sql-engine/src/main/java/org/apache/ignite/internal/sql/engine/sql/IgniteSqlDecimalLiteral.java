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

import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.jetbrains.annotations.Nullable;

/**
 * A decimal SQL literal.
 * <pre>
 *     DECIMAL '&lt;numeric-value&gt;'
 * </pre>
 */
public final class IgniteSqlDecimalLiteral extends SqlNumericLiteral {

    /**
     * Constructor.
     */
    private IgniteSqlDecimalLiteral(BigDecimal value, SqlParserPos pos) {
        super(value, getPrecision(value), value.scale(), true, pos);
    }

    /** Creates a decimal literal. */
    public static IgniteSqlDecimalLiteral create(BigDecimal value, SqlParserPos pos) {
        return new IgniteSqlDecimalLiteral(value, pos);
    }

    /** {@inheritDoc} **/
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DECIMAL");

        var value = getDecimalValue();
        var strVal = writer.getDialect().quoteStringLiteral(value.toString());

        writer.literal(strVal);
    }

    /** {@inheritDoc} **/
    @Override
    public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
        var value = getDecimalValue();
        var precision = getPrecision(value);

        return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, value.scale());
    }

    /** {@inheritDoc} **/
    @Override
    public SqlNumericLiteral clone(SqlParserPos pos) {
        var value = getDecimalValue();

        return new IgniteSqlDecimalLiteral(value, pos);
    }

    /** {@inheritDoc} **/
    @Override
    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlDecimalLiteral)) {
            return litmus.fail("{} != {}", this, node);
        }

        IgniteSqlDecimalLiteral that = (IgniteSqlDecimalLiteral) node;

        if (that.getDecimalValue().compareTo(getDecimalValue()) != 0) {
            return litmus.fail("{} != {}", this, node);
        }

        return true;
    }

    private BigDecimal getDecimalValue() {
        var value = bigDecimalValue();
        assert value != null : "bigDecimalValue returned null for a subclass exact numeric literal: " + this;
        return value;
    }

    private static int getPrecision(BigDecimal value) {
        int scale = value.scale();

        if (value.precision() == 1 && value.compareTo(BigDecimal.ONE) < 0) {
            // For numbers less than 1 we have different precision between Java's BigDecimal and Calcite:
            // 0.01 - BigDecimal precision=1, scale=2, Calcite: precision=3, scale=2

            return 1 + scale;
        } else {
            return value.precision();
        }
    }
}
