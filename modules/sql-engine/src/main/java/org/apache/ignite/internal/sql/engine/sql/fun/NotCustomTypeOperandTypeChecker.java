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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.ignite.internal.schema.AssemblyException;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;

/**
 * Type checking strategy which makes sure there are no operands of custom types.
 */
public class NotCustomTypeOperandTypeChecker implements SqlSingleOperandTypeChecker {
    /*
    By default, type checkers allows operands of type ANY. The problem is that it in fact may
    represent custom type rather than be placeholder for any possible value. This type checker
    makes sure that operand of type ANY is indeed placeholder for any type rather than operand
    of a custom type.
     */

    @Override
    public boolean checkSingleOperandType(SqlCallBinding callBinding, SqlNode operand, int operandIdx, boolean throwOnFailure) {
        return !(callBinding.getOperandType(operandIdx) instanceof IgniteCustomType);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        for (int i = 0; i < callBinding.getOperandCount(); i++) {
            if (!checkSingleOperandType(callBinding, callBinding.operand(i), i, throwOnFailure)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        throw new AssemblyException("Should not be called");
    }
}
