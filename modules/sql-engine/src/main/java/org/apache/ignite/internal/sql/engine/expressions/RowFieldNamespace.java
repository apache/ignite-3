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

package org.apache.ignite.internal.sql.engine.expressions;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

class RowFieldNamespace implements SqlValidatorNamespace {
    private final SqlValidator validator;
    private final RelDataType type;

    RowFieldNamespace(SqlValidator validator, RelDataType type) {
        this.validator = validator;
        this.type = type;
    }

    @Override
    public SqlValidator getValidator() {
        return validator;
    }

    @Override
    public @Nullable SqlValidatorTable getTable() {
        return null;
    }

    @Override
    public RelDataType getRowType() {
        return type;
    }

    @Override
    public RelDataType getType() {
        return type;
    }

    @Override
    public void setType(RelDataType type) {
        // no-op
    }

    @Override
    public RelDataType getRowTypeSansSystemColumns() {
        return type;
    }

    @Override
    public void validate(RelDataType targetRowType) {
        // valid
    }

    @Override
    public @Nullable SqlNode getNode() {
        return null;
    }

    @Override
    public @Nullable SqlNode getEnclosingNode() {
        return null;
    }

    @Override
    public @Nullable SqlValidatorNamespace lookupChild(String name) {
        return null;
    }

    @Override
    public @Nullable RelDataTypeField field(String name) {
        RelDataType rowType = getRowType();

        return validator.getCatalogReader().nameMatcher().field(rowType, name);
    }

    @Override
    public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs() {
        return List.of();
    }

    @Override
    public SqlMonotonicity getMonotonicity(String columnName) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }

    @Override
    public void makeNullable() {

    }

    @Override
    public <T> @Nullable T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        }

        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> clazz) {
        return clazz.isInstance(this);
    }

    @Override
    public SqlValidatorNamespace resolve() {
        return this;
    }

    @Override
    public boolean supportsModality(SqlModality modality) {
        return true;
    }
}
