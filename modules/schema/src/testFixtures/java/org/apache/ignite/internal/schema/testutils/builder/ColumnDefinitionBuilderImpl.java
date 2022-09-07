/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.testutils.builder;

import java.util.Map;
import org.apache.ignite.internal.schema.definition.ColumnDefinitionImpl;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.DefaultValueDefinition;
import org.jetbrains.annotations.Nullable;

/**
 * Column builder.
 */
class ColumnDefinitionBuilderImpl implements ColumnDefinitionBuilder {
    /** Column name. */
    private final String colName;

    /** Column type. */
    private final ColumnType colType;

    /** Nullable flag. */
    private boolean nullable;

    /** Default value expression. */
    private @Nullable Object defValExpr;

    /**
     * Constructor.
     *
     * @param colName Column name.
     * @param colType Column type.
     */
    public ColumnDefinitionBuilderImpl(String colName, ColumnType colType) {
        this.colName = IgniteObjectName.parse(colName);
        this.colType = colType;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDefinitionBuilderImpl asNullable(boolean nullable) {
        this.nullable = nullable;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDefinitionBuilderImpl withDefaultValue(@Nullable Object defValExpr) {
        this.defValExpr = defValExpr;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDefinitionBuilderImpl withHints(Map<String, String> hints) {
        // No op.

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDefinition build() {
        var defaultSupp = defValExpr == null
                ? DefaultValueDefinition.nullValue()
                : DefaultValueDefinition.constant(defValExpr);
        return new ColumnDefinitionImpl(colName, colType, nullable, defaultSupp);
    }
}
