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
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.sql.engine.util.Commons;

abstract class CommonFieldGetter implements InputGetter {
    protected final Expression hnd;

    protected final Expression row;

    protected final RelDataType rowType;

    CommonFieldGetter(Expression hnd, Expression row, RelDataType rowType) {
        this.hnd = hnd;
        this.row = row;
        this.rowType = rowType;
    }

    protected abstract Expression fillExpressions(BlockBuilder list, int index);

    /** {@inheritDoc} */
    @Override
    public Expression field(BlockBuilder list, int index, Type desiredType) {
        Expression fldExpression = fillExpressions(list, index);

        Type fieldType = Commons.typeFactory().getJavaClass(rowType.getFieldList().get(index).getType());

        if (desiredType == null) {
            desiredType = fieldType;
            fieldType = Object.class;
        } else if (fieldType != java.sql.Date.class
                && fieldType != java.sql.Time.class
                && fieldType != java.sql.Timestamp.class) {
            fieldType = Object.class;
        }

        return EnumUtils.convert(fldExpression, fieldType, desiredType);
    }
}
