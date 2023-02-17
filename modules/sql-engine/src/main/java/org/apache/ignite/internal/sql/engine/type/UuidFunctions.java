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

package org.apache.ignite.internal.sql.engine.type;

import java.lang.reflect.Method;
import java.util.UUID;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;

/**
 * A set functions required by expression execution runtime to support of {@code UUID} type.
 */
public final class UuidFunctions {

    /**
     * Implementation of a CAST operator for {@link UuidType} used by expression execution runtime.
     *
     * @see #cast(Object)
     **/
    private static final Method CAST;

    static {
        try {
            CAST = UuidFunctions.class.getMethod("cast", Object.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("cast method is not defined", e);
        }
    }

    private UuidFunctions() {

    }

    /**
     * Creates a cast expression that convert the given operation into {@link UuidType}.
     *
     * @param operand An operand.
     * @return A cast to UUID expression.
     */
    public static Expression cast(Expression operand) {
        return Expressions.call(CAST, operand);
    }

    /**
     * Performs casts from object to {@code UUID}. Accepts values that are Strings, UUIDs.
     *
     * @param value A value.
     * @return An UUID.
     * @throws ClassCastException if type can not be converted to UUID.
     */
    public static UUID cast(Object value) {
        // It would be better to generate Expression tree that is equivalent to the code below
        // from type checking rules for this type in order to avoid code duplication.
        if (value instanceof String) {
            return UUID.fromString((String) value);
        } else {
            return (UUID) value;
        }
    }
}
