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

package org.apache.ignite.internal.sql.engine.rex;

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Extension of {@link RexBuilder}.
 */
public class IgniteRexBuilder extends RexBuilder {
    public static final IgniteRexBuilder INSTANCE = new IgniteRexBuilder(IgniteTypeFactory.INSTANCE);

    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    private IgniteRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    /** {@inheritDoc} **/
    @Override
    public RexNode makeLiteral(@Nullable Object value, RelDataType type, boolean allowCast, boolean trim) {
        if (value != null) {
            if (type.getSqlTypeName() == SqlTypeName.CHAR) {
                if (type.isNullable()) {
                    RelDataType typeNotNull =
                            typeFactory.createTypeWithNullability(type, false);
                    if (allowCast) {
                        RexNode literalNotNull = makeLiteral(value, typeNotNull, allowCast);
                        return makeAbstractCast(type, literalNotNull, false);
                    }
                }

                NlsString string;
                if (value instanceof NlsString) {
                    string = (NlsString) value;
                } else {
                    assert type.getCharset() != null : type + ".getCharset() must not be null";
                    string = new NlsString((String) value, type.getCharset().name(), type.getCollation());
                }

                return makeCharLiteral(string);
            } else if (type.getSqlTypeName() == SqlTypeName.BINARY) {
                return makeBinaryLiteral((ByteString) value);
            } else if (value instanceof String) {
                if (type.getSqlTypeName() == SqlTypeName.DOUBLE) {
                    value = Double.parseDouble((String) value);
                } else if (type.getSqlTypeName() == SqlTypeName.REAL || type.getSqlTypeName() == SqlTypeName.FLOAT) {
                    value = Float.parseFloat((String) value);
                }
            }
        }

        if (value instanceof UUID && type.getSqlTypeName() == SqlTypeName.UUID) {
            // Generic super.makeLiteral() lacks handling of UUID type, therefore we will
            // WA this on our side.
            return makeUuidLiteral((UUID) value);
        }

        return super.makeLiteral(value, type, allowCast, trim);
    }

    @Override
    public RexNode makeCast(
            SqlParserPos pos,
            RelDataType type,
            RexNode exp,
            boolean matchNullability,
            boolean safe,
            RexLiteral format) {
        if (exp instanceof RexLiteral) {
            // Override cast for DECIMAL from INTERVAL literal, to be consistent with INTERVAL call behavior
            if (SqlTypeUtil.isExactNumeric(type) && SqlTypeUtil.isInterval(exp.getType())) {
                return makeCastIntervalToExact(pos, type, exp);
            }
        }

        return super.makeCast(pos, type, exp, matchNullability, safe, format);
    }

    // Copy-pasted from RexBuilder.
    @SuppressWarnings("MethodOverridesInaccessibleMethodOfSuper")
    private RexNode makeCastIntervalToExact(SqlParserPos pos, RelDataType toType, RexNode exp) {
        TimeUnit endUnit = exp.getType().getSqlTypeName().getEndUnit();
        TimeUnit baseUnit = baseUnit(exp.getType().getSqlTypeName());
        BigDecimal multiplier = baseUnit.multiplier;
        BigDecimal divider = endUnit.multiplier;
        RexNode value =
                multiplyDivide(pos, decodeIntervalOrDecimal(pos, exp), multiplier, divider);
        return ensureType(pos, toType, value, false);
    }
}
