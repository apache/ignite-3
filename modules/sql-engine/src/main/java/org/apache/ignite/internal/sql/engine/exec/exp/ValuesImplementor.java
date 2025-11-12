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

import static org.apache.ignite.internal.sql.engine.util.RexUtils.literalValue;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.StructNativeType;

/** Implementor which implements {@link SqlScalar} returning list of rows. */
class ValuesImplementor {
    private final JavaTypeFactory typeFactory;

    ValuesImplementor(JavaTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    /**
     * Implements given two-dimensional list of literals as {@link SqlScalar} returning list of rows.
     *
     * @param values The two-dimensional list of literals to be converted to rows.
     * @param rowType The type of the row represented by given literal expressions.
     * @param <RowT> The type of the execution row.
     * @return An implementation of scalar.
     * @see SqlScalar
     */
    <RowT> SqlScalar<RowT, List<RowT>> implement(List<List<RexLiteral>> values, RelDataType rowType) {
        List<RelDataType> typeList = RelOptUtil.getFieldTypeList(rowType);

        StructNativeType structNativeType = TypeUtils.structuredTypeFromRelTypeList(typeList);
        List<Class<?>> types = Commons.transform(typeList, type -> Primitives.wrap((Class<?>) typeFactory.getJavaClass(type)));

        return context -> {
            List<RowT> results = new ArrayList<>(values.size());
            RowBuilder<RowT> rowBuilder = context.rowHandler().factory(structNativeType).rowBuilder();

            for (List<RexLiteral> row : values) {
                for (int i = 0; i < types.size(); i++) {
                    rowBuilder.addField(literalValue(context, row.get(i), types.get(i)));
                }

                results.add(rowBuilder.buildAndReset());
            }

            return results;
        };
    }
}
