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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;

/**
 * IgniteResource interface.
 */
public interface IgniteResource {
    IgniteResource INSTANCE = Resources.create(IgniteResource.class);

    @Resources.BaseMessage("Illegal alias. {0} is reserved name")
    Resources.ExInst<SqlValidatorException> illegalAlias(String a0);

    @Resources.BaseMessage("Cannot update field \"{0}\". Primary key columns are not modifiable")
    Resources.ExInst<SqlValidatorException> cannotUpdateField(String field);

    @Resources.BaseMessage("Illegal aggregate function. {0} is unsupported at the moment")
    Resources.ExInst<SqlValidatorException> unsupportedAggregationFunction(String a0);

    @Resources.BaseMessage("Illegal value of {0}. The value must be positive and less than Integer.MAX_VALUE (" + Integer.MAX_VALUE + ")")
    Resources.ExInst<SqlValidatorException> correctIntegerLimit(String a0);

    @Resources.BaseMessage("Invalid decimal literal")
    Resources.ExInst<SqlValidatorException> decimalLiteralInvalid();

    @Resources.BaseMessage
            ("Values passed to {0} operator must have compatible types. Dynamic parameter requires adding explicit type cast")
    Resources.ExInst<SqlValidatorException> operationRequiresExplicitCast(String operation);

    @Resources.BaseMessage("Assignment from {0} to {1} can not be performed. Dynamic parameter requires adding explicit type cast")
    Resources.ExInst<SqlValidatorException> assignmentRequiresExplicitCast(String type1, String type2);

    @Resources.BaseMessage("System view {0} is not modifiable")
    ExInst<SqlValidatorException> systemViewIsNotModifiable(String systemViewName);

    @Resources.BaseMessage("Ambiguous operator {0}. Dynamic parameter requires adding explicit type cast")
    Resources.ExInst<SqlValidatorException> ambiguousOperator1(String signature);

    @Resources.BaseMessage("Ambiguous operator {0}. Dynamic parameter requires adding explicit type cast. Supported form(s): \n{1}")
    Resources.ExInst<SqlValidatorException> ambiguousOperator2(String signature, String allowedSignatures);

    @Resources.BaseMessage("Unable to determine type of a dynamic parameter. Dynamic parameter requires adding explicit type cast")
    Resources.ExInst<SqlValidatorException> unableToResolveDynamicParameterType();

    @Resources.BaseMessage("Incorrect type of a dynamic parameter. Expected <{0}> but got <{1}>")
    Resources.ExInst<SqlValidatorException> incorrectDynamicParameterType(String expected, String actual);

    @Resources.BaseMessage("Expression is not supported: {0}")
    Resources.ExInst<SqlValidatorException> unsupportedExpression(String exprType);

    @Resources.BaseMessage("CHAR datatype is not supported in table")
    Resources.ExInst<SqlValidatorException> charDataTypeIsNotSupportedInTable();

    @Resources.BaseMessage("Length for type {0} must be at least 1")
    Resources.ExInst<SqlValidatorException> invalidStringLength(String typeName);

    /** Constructs a signature string to use in error messages. */
    static String makeSignature(SqlCallBinding binding, RelDataType... operandTypes) {
        return makeSignature(binding, Arrays.asList(operandTypes));
    }

    /** Constructs a signature string to use in error messages. */
    static String makeSignature(SqlCallBinding binding, List<RelDataType> operandTypes) {
        IgniteSqlValidator validator = (IgniteSqlValidator) binding.getValidator();
        List<SqlNode> operandList = binding.getCall().getOperandList();

        return makeSignature(binding.getOperator(), validator, operandList, operandTypes);
    }

    /** Constructs a signature string to use in error messages. */
    static String makeSignature(SqlOperator operator, IgniteSqlValidator validator,
            List<SqlNode> operands, List<RelDataType> originalOperandTypes) {

        List<String> operandTypeNames = new ArrayList<>(originalOperandTypes.size());

        // Replace types for unspecified parameters with UNKNOWN type.

        for (int i = 0; i < operands.size(); i++) {
            SqlNode node = operands.get(i);
            RelDataType dataType;

            if (validator.isUnspecifiedDynamicParam(node)) {
                dataType = validator.getUnknownType();
            } else {
                dataType = originalOperandTypes.get(i);
            }

            operandTypeNames.add(format("<{}>", dataType));
        }

        SqlKind kind = operator.getKind();
        if (SqlKind.BINARY_ARITHMETIC.contains(kind) || SqlKind.BINARY_COMPARISON.contains(kind)) {
            return format("{} {} {}", operandTypeNames.get(0), operator.getName(), operandTypeNames.get(1));
        } else if (operator.getName().startsWith("IS ")) {
            // IS NULL, IS NOT NULL, etc.
            return format("{} {}", operandTypeNames.get(0), operator.getName());
        } else if (kind == SqlKind.BETWEEN) {
            return format("{} {} AND {}", operator.getName(), operandTypeNames.get(0), operandTypeNames.get(1));
        } else if (kind == SqlKind.MINUS_PREFIX) {
            // -
            return format("{}{}", operator.getName(), operandTypeNames.get(0));
        } else {
            // Other operators
            String operandStr = String.join(", ", operandTypeNames);
            return format("{}({})", operator.getName(), operandStr);
        }
    }
}
