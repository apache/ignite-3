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

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.util.IgniteMethod;

class BiFieldGetter extends CommonFieldGetter {
    private final Expression row2;

    BiFieldGetter(Expression hnd, Expression row1, Expression row2, RelDataType rowType) {
        super(hnd, row1, rowType);
        this.row2 = row2;
    }

    /** {@inheritDoc} */
    @Override
    protected Expression fillExpressions(BlockBuilder list, int index) {
        Expression row1 = list.append("row1", this.row);
        Expression row2 = list.append("row2", this.row2);

        Expression field = Expressions.call(
                IgniteMethod.ROW_HANDLER_BI_GET.method(), hnd,
                Expressions.constant(index), row1, row2);

        return field;
    }
}
