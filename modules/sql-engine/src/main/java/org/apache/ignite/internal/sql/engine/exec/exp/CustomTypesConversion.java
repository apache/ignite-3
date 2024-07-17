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

package org.apache.ignite.internal.sql.engine.exec.exp;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * Provides cast expressions for {@link IgniteCustomType custom data types}.
 */
final class CustomTypesConversion {

    static final CustomTypesConversion INSTANCE = new CustomTypesConversion(Commons.typeFactory());

    private final Map<Class<?>, Function<Expression, Expression>> castByInternalType = new HashMap<>();

    private final Map<String, Function<Expression, Expression>> castByTypeName = new HashMap<>();

    private CustomTypesConversion(IgniteTypeFactory typeFactory) {
        // IgniteCustomType: Add cast function implementations.
        var customTypeSpecs = typeFactory.getCustomTypeSpecs();

        for (var spec : customTypeSpecs.values()) {
            // We use Object as an argument because:
            //
            // 1. type info is erased for dynamic parameters. See DataContextInputGetter in RexExecutorImpl.
            // 2. so internal calls will throw a CastCastException instead of a NoSuchMethodError in runtime
            // (It would be even better if such casts were not necessary).

            Function<Expression, Expression> castExpr =
                    (operand) ->  Expressions.call(spec.castFunction(), Expressions.convert_(operand, Object.class));
            // ??? Should we add a hook here that can short-circuit when a custom type can not be converted?

            castByInternalType.put(spec.storageType(), castExpr);
            castByTypeName.put(spec.typeName(), castExpr);
        }
    }

    /**
     * If the specified {@code internalType} represents is a custom data type,
     * then this method returns expression that converts an operand to it. Otherwise returns {@code null}.
     * */
    @Nullable
    Expression tryConvert(Expression operand, Type internalType) {
        // internal type is always a Class.
        var cast = castByInternalType.get((Class<?>) internalType);
        if (cast == null) {
            return null;
        }

        return cast.apply(operand);
    }

    /**
     * If the specified {@code targetType} represents is a custom data type,
     * then this method returns expression that converts an operand to it. Otherwise returns {@code null}.
     * */
    @Nullable
    Expression tryConvert(Expression operand, RelDataType targetType) {
        if (!(targetType instanceof IgniteCustomType)) {
            return null;
        }

        var customType = (IgniteCustomType) targetType;
        Function<Expression, Expression> cast = castByTypeName.get(customType.getCustomTypeName());

        return cast.apply(operand);
    }
}
