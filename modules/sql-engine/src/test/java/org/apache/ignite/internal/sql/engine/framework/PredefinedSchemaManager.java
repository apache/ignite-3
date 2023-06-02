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

package org.apache.ignite.internal.sql.engine.framework;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy wrapper for predefined collection of schemas.
 *
 * <p>Accepts collection of {@link IgniteSchema schemas} as parameter and implements required
 * methods of {@link SqlSchemaManager} around them. Assumes given schemas will never be changed.
 *
 * @see IgniteSchema
 * @see SqlSchemaManager
 */
public class PredefinedSchemaManager implements SqlSchemaManager {
    private final SchemaPlus root;
    private final Map<Integer, IgniteTable> tableById;

    /** Constructs schema manager from a single schema. */
    PredefinedSchemaManager(IgniteSchema schema) {
        this(List.of(schema));
    }

    /** Constructs schema manager from a collection of schemas. */
    PredefinedSchemaManager(Collection<IgniteSchema> schemas) {
        this.root = Frameworks.createRootSchema(false);
        this.tableById = new HashMap<>();

        for (IgniteSchema schema : schemas) {
            root.add(schema.getName(), schema);

            tableById.putAll(
                    schema.getTableNames().stream()
                            .map(schema::getTable)
                            .map(IgniteTable.class::cast)
                            .collect(Collectors.toMap(IgniteTable::id, Function.identity()))
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(@Nullable String schema) {
        return schema == null ? root : root.getSubSchema(schema);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus schema(String name, int version) {
        return schema(name);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> actualSchemaAsync(long ver) {
        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaPlus activeSchema(@Nullable String name, long timestamp) {
        return schema(name);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTable tableById(int id) {
        return tableById.get(id);
    }
}
