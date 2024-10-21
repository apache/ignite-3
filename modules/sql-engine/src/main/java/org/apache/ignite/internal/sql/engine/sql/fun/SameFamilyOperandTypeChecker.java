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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;

/**
 * Type checking strategy which verifies that types belongs to the same type family.
 */
public class SameFamilyOperandTypeChecker extends SameOperandTypeChecker {

    SameFamilyOperandTypeChecker(int countOfOperands) {
        super(countOfOperands);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        // Coerce type first.
        if (callBinding.isTypeCoercionEnabled()) {
            TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
            // For comparison operators, i.e. >, <, =, >=, <=.
            typeCoercion.binaryComparisonCoercion(callBinding);
        }

        boolean result = doCheckOperandTypes(callBinding);

        if (!result && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return result;
    }

    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

    private boolean doCheckOperandTypes(SqlOperatorBinding operatorBinding) {
        int actualCountOfOperands = nOperands;
        if (actualCountOfOperands == -1) {
            actualCountOfOperands = operatorBinding.getOperandCount();
        }

        RelDataType[] types = new RelDataType[actualCountOfOperands];
        List<Integer> operandList = getOperandList(operatorBinding.getOperandCount());
        for (int i : operandList) {
            types[i] = operatorBinding.getOperandType(i);
        }

        int prev = -1;
        for (int i : operandList) {
            if (prev >= 0) {
                if (!TypeUtils.typeFamiliesAreCompatible(Commons.typeFactory(), types[i], types[prev])) {
                    return false;
                }
            }
            prev = i;
        }

        return true;
    }
}
