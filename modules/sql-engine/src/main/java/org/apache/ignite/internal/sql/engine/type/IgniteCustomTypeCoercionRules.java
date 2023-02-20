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

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.ignite.internal.sql.engine.prepare.IgniteTypeCoercion;

/**
 * Defines rules for coercing {@link SqlTypeName built-in types} to {@link IgniteCustomType custom data types}.
 *
 * @see IgniteTypeCoercion ignite type coercion.
 * @see TypeCoercion calcite's TypeCoercion interface.
 */
public final class IgniteCustomTypeCoercionRules {

    private final Map<String, Set<SqlTypeName>> canCastFrom;

    IgniteCustomTypeCoercionRules(Map<String, IgniteCustomTypeSpec> typeSpecs, Map<String, Set<SqlTypeName>> canCastFrom) {
        for (var rule : canCastFrom.entrySet()) {
            var typeName = rule.getKey();
            if (!typeSpecs.containsKey(typeName)) {
                var error = format("Unable to define type coercion rule. "
                        + "Unexpected custom type: {}. Rules: {}. Known types: {}", typeName, rule.getValue(), typeSpecs.keySet()
                );
                throw new IllegalArgumentException(error);
            }
        }

        this.canCastFrom = canCastFrom;
    }

    /**
     * Checks whether the given custom data type can be converted from the specified type.
     *
     * @param typeName Type name.
     * @param fromType Another data type.
     */
    public boolean needToCast(String typeName, RelDataType fromType) {
        // The implementation of this method must all use ::canCastFrom(typeName).
        // because the former is used to generate rules for runtime execution.
        var rules = canCastFrom(typeName);

        return rules.contains(fromType.getSqlTypeName());
    }

    private Set<SqlTypeName> canCastFrom(String typeName) {
        return canCastFrom.getOrDefault(typeName, Collections.emptySet());
    }

    /** Creates a builder to define a table of type coercion rules. **/
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link IgniteCustomTypeCoercionRules}.
     */
    public static class Builder {

        private final Map<String, Set<SqlTypeName>> canCastFrom = new HashMap<>();

        Builder() {

        }

        /**
         * Adds a rule that allows cast from the given custom type to the specified built-in type.
         */
        public Builder addRule(String typeName, SqlTypeName to) {
            var rules = canCastFrom.computeIfAbsent(typeName, (k) -> EnumSet.noneOf(SqlTypeName.class));
            rules.add(to);
            return this;
        }

        /**
         * Adds rules that allow casts from the given custom type to the specified built-in types.
         */
        public Builder addRules(String typeName, Collection<SqlTypeName> typeNames) {
            var rules = canCastFrom.computeIfAbsent(typeName, (k) -> EnumSet.noneOf(SqlTypeName.class));
            rules.addAll(typeNames);
            return this;
        }

        /**
         * Builds a table of type coercion rules for the given custom data types.
         *
         * @param typeSpecs A map of custom type specs.
         * @return  A table of type coercion rules.
         *
         * @throws IllegalArgumentException if a this builder contains rules for custom types not present in {@code typeSpecs}.
         */
        public IgniteCustomTypeCoercionRules build(Map<String, IgniteCustomTypeSpec> typeSpecs) {
            return new IgniteCustomTypeCoercionRules(typeSpecs, canCastFrom);
        }
    }
}
