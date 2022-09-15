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

package org.apache.ignite.internal.schema.definition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.index.ColumnarIndexDefinition;
import org.apache.ignite.schema.definition.index.IndexColumnDefinition;
import org.apache.ignite.schema.definition.index.IndexDefinition;

/**
 * Schema validation methods.
 */
public class SchemaValidationUtils {
    /**
     * Validate primary key.
     *
     * @param pkColNames Primary key columns.
     * @param cols       Table columns.
     */
    public static void validatePrimaryKey(Set<String> pkColNames, final Map<String, ColumnDefinition> cols) {
        pkColNames.stream()
                .filter(pkCol -> cols.get(pkCol).nullable())
                .findAny()
                .ifPresent((pkCol) -> {
                    throw new IllegalStateException("Primary key cannot contain nullable column [col=" + pkCol + "].");
                });
    }

    /**
     * Validate indices.
     *
     * @param indices Table indices.
     * @param cols Table columns.
     * @param colocationColNames Colocation columns names.
     */
    public static void validateIndices(
            Collection<IndexDefinition> indices,
            Collection<ColumnDefinition> cols,
            List<String> colocationColNames) {
        Set<String> colNames = cols.stream().map(ColumnDefinition::name).collect(Collectors.toSet());

        for (IndexDefinition idx : indices) {
            assert idx instanceof ColumnarIndexDefinition : "Only columnar indices are supported.";
            // Note: E.g. functional index is not columnar index as it index an expression result only.

            ColumnarIndexDefinition idx0 = (ColumnarIndexDefinition) idx;

            if (!idx0.columns().stream().map(IndexColumnDefinition::name).allMatch(colNames::contains)) {
                throw new IllegalStateException("Index column must exist in the schema.");
            }

            if (idx0.unique() && !(idx0.columns().stream().map(IndexColumnDefinition::name).allMatch(colocationColNames::contains))) {
                throw new IllegalStateException("Unique index must contains all colocation columns.");
            }
        }
    }
}
