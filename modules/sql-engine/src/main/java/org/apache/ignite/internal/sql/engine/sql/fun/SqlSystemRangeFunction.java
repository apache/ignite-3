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

import static org.apache.calcite.sql.type.InferTypes.ANY_NULLABLE;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.TableFunctionImpl;

/**
 * Definition of the "SYSTEM_RANGE" builtin SQL function.
 */
public class SqlSystemRangeFunction extends SqlUserDefinedTableFunction {
    /**
     * Creates the SqlSystemRangeFunction.
     */
    SqlSystemRangeFunction(Method method, String funcName) {
        super(
                new SqlIdentifier(funcName, SqlParserPos.ZERO),
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                ANY_NULLABLE,
                Arg.metadata(
                        Arg.of("from", f -> f.createSqlType(SqlTypeName.BIGINT),
                                SqlTypeFamily.EXACT_NUMERIC, false),
                        Arg.of("to", f -> f.createSqlType(SqlTypeName.BIGINT),
                                SqlTypeFamily.EXACT_NUMERIC, false),
                        Arg.of("step", f -> f.createSqlType(SqlTypeName.BIGINT),
                                SqlTypeFamily.EXACT_NUMERIC, true)),
                TableFunctionImpl.create(method)
        );
    }

    /** {@inheritDoc} */
    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.MONOTONIC;
    }

    /** Operands checker helper interface. */
    private interface Arg {

        String name();

        RelDataType type(RelDataTypeFactory typeFactory);

        SqlTypeFamily family();

        boolean optional();

        static SqlOperandMetadata metadata(Arg... args) {
            return OperandTypes.operandMetadata(
                    Arrays.stream(args).map(Arg::family).collect(Collectors.toList()),
                    typeFactory ->
                            Arrays.stream(args).map(arg -> arg.type(typeFactory))
                                    .collect(Collectors.toList()),
                    i -> args[i].name(), i -> args[i].optional());
        }

        static Arg of(
                String name,
                Function<RelDataTypeFactory, RelDataType> protoType,
                SqlTypeFamily family,
                boolean optional
        ) {
            return new Arg() {
                @Override public String name() {
                    return name;
                }

                @Override public RelDataType type(RelDataTypeFactory typeFactory) {
                    return protoType.apply(typeFactory);
                }

                @Override public SqlTypeFamily family() {
                    return family;
                }

                @Override public boolean optional() {
                    return optional;
                }
            };
        }
    }
}
