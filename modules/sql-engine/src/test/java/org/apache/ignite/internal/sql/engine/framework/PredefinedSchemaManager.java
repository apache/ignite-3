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

import static java.util.function.Function.identity;
import static org.apache.ignite.internal.util.CollectionUtils.toIntMapCollector;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchemas;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;

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
    private final IgniteSchemas root;
    private final Int2ObjectMap<IgniteTable> tableById;
    private final SchemaPlus schemaPlus;

    /** Constructs schema manager from a single schema. */
    public PredefinedSchemaManager(IgniteSchema schema) {
        this(List.of(schema));
    }

    /** Constructs schema manager from a collection of schemas. */
    public PredefinedSchemaManager(Collection<IgniteSchema> schemas) {
        SchemaPlus schemaPlus = Frameworks.createRootSchema(false);
        this.tableById = new Int2ObjectOpenHashMap<>();

        for (IgniteSchema schema : schemas) {
            schemaPlus.add(schema.getName(), schema);

            tableById.putAll(
                    schema.getTableNames().stream()
                            .map(schema::getTable)
                            .map(IgniteTable.class::cast)
                            .collect(toIntMapCollector(IgniteTable::id, identity()))
            );
        }

        this.schemaPlus = schemaPlus;

        root = new IgniteSchemas(schemaPlus, 0);
    }

    SchemaPlus rootSchema() {
        return schemaPlus;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSchemas schemas(int catalogVersion) {
        return root;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSchemas schemas(long timestamp) {
        return root;
    }

    /** {@inheritDoc} */
    @Override
    public int catalogVersion(long timestamp) {
        return root.catalogVersion();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> schemaReadyFuture(int catalogVersion) {
        return nullCompletedFuture();
    }

    @Override
    public IgniteTable table(int catalogVersion, int tableId) {
        IgniteTable table = tableById.get(tableId);

        if (table == null) {
            throw new RuntimeException("Table not found: " + tableId);
        }

        return table;
    }
}
