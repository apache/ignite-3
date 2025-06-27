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

package org.apache.ignite.internal.sql.engine.prepare;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;

/**
 * Implementation of {@link SqlRexConvertletTable}.
 */
public class IgniteConvertletTable extends ReflectiveConvertletTable {
    public static final IgniteConvertletTable INSTANCE = new IgniteConvertletTable();

    private IgniteConvertletTable() {
        // Replace Calcite's convertlet with our own.
        registerOp(SqlStdOperatorTable.TIMESTAMP_DIFF, new TimestampDiffConvertlet());

        // Replace Calcite's alias so it can pass "call to wrong operator" precondition in ReflectiveConvertletTable.addAlias
        addAlias(IgniteSqlOperatorTable.PERCENT_REMAINDER, SqlStdOperatorTable.MOD);

        // Replace plus/minus implementors, because Calcite missed the TIMESTAMP WITH LOCAL TIME ZONE data type.
        registerOp(SqlStdOperatorTable.PLUS, this::convertPlus);
        registerOp(SqlStdOperatorTable.MINUS, this::convertMinus);
    }

    /** {@inheritDoc} */
    @Override public SqlRexConvertlet get(SqlCall call) {
        SqlRexConvertlet res = super.get(call);

        return res == null ? StandardConvertletTable.INSTANCE.get(call) : res;
    }

    /** Convertlet that handles the {@code TIMESTAMPDIFF} function. */
    private static class TimestampDiffConvertlet implements SqlRexConvertlet {
        /** {@inheritDoc} */
        @Override public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            // TIMESTAMPDIFF(unit, t1, t2)
            //    => (t2 - t1) UNIT
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final SqlIntervalQualifier unitLiteral = call.operand(0);
            TimeUnit unit = unitLiteral.getUnit();
            BigDecimal multiplier = BigDecimal.ONE;
            BigDecimal divider = BigDecimal.ONE;
            SqlTypeName sqlTypeName = unit == TimeUnit.NANOSECOND
                    ? SqlTypeName.BIGINT
                    : SqlTypeName.INTEGER;
            switch (unit) {
                case MICROSECOND:
                case MILLISECOND:
                case NANOSECOND:
                    divider = unit.multiplier;
                    unit = TimeUnit.MILLISECOND;
                    break;
                case WEEK:
                    multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND);
                    divider = unit.multiplier;
                    unit = TimeUnit.SECOND;
                    break;
                case QUARTER:
                    divider = unit.multiplier;
                    unit = TimeUnit.MONTH;
                    break;
                default:
                    break;
            }
            final SqlIntervalQualifier qualifier =
                    new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
            final RexNode op2 = cx.convertExpression(call.operand(2));
            final RexNode op1 = cx.convertExpression(call.operand(1));
            final RelDataType intervalType =
                    cx.getTypeFactory().createTypeWithNullability(
                            cx.getTypeFactory().createSqlIntervalType(qualifier),
                            op1.getType().isNullable() || op2.getType().isNullable());
            final RexCall rexCall = (RexCall) rexBuilder.makeCall(
                    intervalType, SqlStdOperatorTable.MINUS_DATE,
                    List.of(op2, op1));
            final RelDataType intType =
                    cx.getTypeFactory().createTypeWithNullability(
                            cx.getTypeFactory().createSqlType(sqlTypeName),
                            SqlTypeUtil.containsNullable(rexCall.getType()));

            RexNode e;

            // Since Calcite converts internal time representation to seconds during cast we need our own cast
            // method to keep fraction of seconds.
            if (unit == TimeUnit.MILLISECOND) {
                e = makeCastMilliseconds(rexBuilder, intType, rexCall);
            } else {
                e = rexBuilder.makeCast(intType, rexCall);
            }

            return rexBuilder.multiplyDivide(e, multiplier, divider);
        }

        /**
         * Creates a call to cast milliseconds interval.
         */
        static RexNode makeCastMilliseconds(RexBuilder builder, RelDataType type, RexNode exp) {
            return builder.ensureType(type, builder.decodeIntervalOrDecimal(exp), false);
        }
    }

    /** Convertlet that handles the {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} data type. */
    private RexNode convertPlus(
            IgniteConvertletTable this,
            SqlRexContext cx, SqlCall call) {
        final RexNode rex = StandardConvertletTable.INSTANCE.convertCall(cx, call);
        switch (rex.getType().getSqlTypeName()) {
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Use special "+" operator for datetime + interval.
                // Re-order operands, if necessary, so that interval is second.
                final RexBuilder rexBuilder = cx.getRexBuilder();
                List<RexNode> operands = ((RexCall) rex).getOperands();
                if (operands.size() == 2) {
                    final SqlTypeName sqlTypeName = operands.get(0).getType().getSqlTypeName();
                    switch (sqlTypeName) {
                        case INTERVAL_YEAR:
                        case INTERVAL_YEAR_MONTH:
                        case INTERVAL_MONTH:
                        case INTERVAL_DAY:
                        case INTERVAL_DAY_HOUR:
                        case INTERVAL_DAY_MINUTE:
                        case INTERVAL_DAY_SECOND:
                        case INTERVAL_HOUR:
                        case INTERVAL_HOUR_MINUTE:
                        case INTERVAL_HOUR_SECOND:
                        case INTERVAL_MINUTE:
                        case INTERVAL_MINUTE_SECOND:
                        case INTERVAL_SECOND:
                            operands = ImmutableList.of(operands.get(1), operands.get(0));
                            break;
                        default:
                            break;
                    }
                }
                return rexBuilder.makeCall(call.getParserPosition(), rex.getType(),
                        SqlStdOperatorTable.DATETIME_PLUS, operands);
            default:
                return rex;
        }
    }

    /** Convertlet that handles the {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE} data type. */
    private RexNode convertMinus(
            IgniteConvertletTable this,
            SqlRexContext cx, SqlCall call) {
        RexCall e =
                (RexCall) StandardConvertletTable.INSTANCE.convertCall(cx, call);
        switch (e.getOperands().get(0).getType().getSqlTypeName()) {
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return StandardConvertletTable.INSTANCE.convertDatetimeMinus(cx, SqlStdOperatorTable.MINUS_DATE,
                        call);
            default:
                return e;
        }
    }
}
