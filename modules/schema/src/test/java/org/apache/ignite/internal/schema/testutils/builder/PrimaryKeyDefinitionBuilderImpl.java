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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.definition.index.PrimaryKeyDefinitionImpl;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.apache.ignite.schema.definition.PrimaryKeyDefinition;

/**
 * Primary key builder.
 */
class PrimaryKeyDefinitionBuilderImpl implements SchemaObjectBuilder, PrimaryKeyDefinitionBuilder {
    /** Index columns. */
    @IgniteToStringInclude
    private List<String> columns;

    /** Colocation columns. */
    @IgniteToStringInclude
    private List<String> colocationColumns;

    /** Builder hints. */
    protected Map<String, String> hints;

    /** {@inheritDoc} */
    @Override
    public PrimaryKeyDefinitionBuilderImpl withColumns(String... columns) {
        this.columns = Arrays.stream(columns).map(IgniteObjectName::parse).collect(Collectors.toList());

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public PrimaryKeyDefinitionBuilderImpl withColumns(List<String> columns) {
        this.columns = columns.stream().map(IgniteObjectName::parse).collect(Collectors.toList());

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public PrimaryKeyDefinitionBuilderImpl withColocationColumns(String... colocationCols) {
        this.colocationColumns = colocationCols == null
                ? null
                : Arrays.stream(colocationCols).map(IgniteObjectName::parse).collect(Collectors.toList());

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public PrimaryKeyDefinitionBuilderImpl withColocationColumns(List<String> colocationCols) {
        this.colocationColumns = colocationCols == null
                ? null
                : colocationCols.stream().map(IgniteObjectName::parse).collect(Collectors.toList());

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public PrimaryKeyDefinitionBuilderImpl withHints(Map<String, String> hints) {
        this.hints = hints;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public PrimaryKeyDefinition build() {
        if (columns == null) {
            throw new IllegalStateException("Primary key column(s) must be configured.");
        }

        Set<String> cols = Set.copyOf(columns);

        List<String> colocationCols;

        if (CollectionUtils.nullOrEmpty(colocationColumns)) {
            colocationCols = List.copyOf(columns);
        } else {
            colocationCols = List.copyOf(colocationColumns);
        }

        return new PrimaryKeyDefinitionImpl(cols, colocationCols);
    }
}
