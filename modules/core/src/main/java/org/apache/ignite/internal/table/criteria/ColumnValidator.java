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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Collection;
import org.apache.ignite.table.criteria.Column;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaVisitor;
import org.apache.ignite.table.criteria.Expression;
import org.apache.ignite.table.criteria.Parameter;
import org.jetbrains.annotations.Nullable;

/**
 * Column validator.
 */
class ColumnValidator implements CriteriaVisitor<Collection<String>> {
    static final ColumnValidator INSTANCE = new ColumnValidator();

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Parameter<T> argument, @Nullable Collection<String> context) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Column column, @Nullable Collection<String> context) {
        String colName = column.getName();

        if (!nullOrEmpty(context) && !context.contains(colName)) {
            throw new IllegalArgumentException("Unexpected column name: " + colName);
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Expression expression, @Nullable Collection<String> context) {
        for (Criteria element : expression.getElements()) {
            element.accept(this, context);
        }
    }

    @Override
    public <T> void visit(Criteria criteria, @Nullable Collection<String> context) {
        criteria.accept(this, context);
    }
}
