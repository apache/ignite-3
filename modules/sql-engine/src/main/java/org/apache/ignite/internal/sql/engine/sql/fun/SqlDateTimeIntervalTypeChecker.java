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

package org.apache.ignite.internal.sql.engine.sql.fun;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Type checking strategy for DATETIME + INTERVAL / INTERVAL + DATETIME / DATETIME - INTERVAL operations.
 * This strategy only permits operands in which all fields of interval operand's type can be included in datetime operand's type. Examples:
 * <ul>
 *     <li>it permits {@code TIME op DAY TO SECOND INTERVALS} but does not allow {@code TIME op INTERVAL_MONTHS},
 *     because the time type consists of hours, minutes, seconds. The day component is ignored.</li>
 *     <li>it permits {@code DATE op INTERVAL (any interval type)}, because the date type includes both years, month, and days.
 *     and that a sub-day interval is converted to day intervals e.g. {@code 25 HOURS} is {@code 1 DAY},
 *     {@code 5 SECONDS} = {@code 0 DAYS}, {@code 1440 MINUTES} is {@code 1 DAY}.
 *     </li>
 *     <li>it permits {@code TIMESTAMP op INTERVAL (any interval type)}, because the timestamp type consists of fields from both the date
 *     and the time types.</li>
 * </ul>
 */
public class SqlDateTimeIntervalTypeChecker implements SqlOperandTypeChecker {

    private static final Map<SqlTypeName, Set<SqlTypeName>> MATCHING_INTERVAL_TYPES = Map.of(
            SqlTypeName.TIME, SqlTypeName.DAY_INTERVAL_TYPES,
            SqlTypeName.DATE, SqlTypeName.INTERVAL_TYPES,
            SqlTypeName.TIMESTAMP, SqlTypeName.INTERVAL_TYPES,
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, SqlTypeName.INTERVAL_TYPES
    );

    private static final SqlOperandCountRange TWO_OPERANDS = SqlOperandCountRanges.of(2);

    private final boolean dateTimeFirst;

    SqlDateTimeIntervalTypeChecker(boolean dateTimeFirst) {
        this.dateTimeFirst = dateTimeFirst;
    }

    /** {@inheritDoc} */
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        assert callBinding.getOperandCount() == 2;

        boolean result = doCheckOperandTypes(callBinding);
        if (!result && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return result;
    }

    private boolean doCheckOperandTypes(SqlCallBinding callBinding) {
        RelDataType t1 = callBinding.getOperandType(0);
        RelDataType t2 = callBinding.getOperandType(1);

        SqlTypeName dateTimeType;
        SqlTypeName intervalType;

        if (dateTimeFirst) {
            // DATETIME <op> INTERVAL
            if (!SqlTypeUtil.isDatetime(t1) || !SqlTypeUtil.isInterval(t2)) {
                return false;
            }
            dateTimeType = t1.getSqlTypeName();
            intervalType = t2.getSqlTypeName();
        } else {
            // INTERVAL <op> DATETIME
            if (!SqlTypeUtil.isInterval(t1) || !SqlTypeUtil.isDatetime(t2)) {
                return false;
            }
            intervalType = t1.getSqlTypeName();
            dateTimeType = t2.getSqlTypeName();
        }

        Set<SqlTypeName> intervalTypes = MATCHING_INTERVAL_TYPES.get(dateTimeType);
        return intervalTypes != null && intervalTypes.contains(intervalType);
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return TWO_OPERANDS;
    }

    /** {@inheritDoc} */
    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        if (dateTimeFirst) {
            return format("'<DATETIME> {} <INTERVAL>'", op.getName());
        } else {
            return format("'<INTERVAL> {} <DATETIME>'", op.getName());
        }
    }
}
