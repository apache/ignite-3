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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.concurrent.CompletableFuture;
import org.apache.calcite.schema.SchemaPlus;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Sql schemas operations interface.
 */
public interface SqlSchemaManager {
    /**
     * Returns schema with given name and by the given version, if name is not specified, returns default schema of the given version.
     *
     * @param schemaName Schema name. If {@code null}, then default schema name will be used.
     * @param version Catalog version.
     */
    SchemaPlus schema(@Nullable String schemaName, int version);

    /**
     * Wait for {@code ver} schema version, just a stub, need to be removed after IGNITE-18733.
     */
    @Deprecated
    CompletableFuture<?> actualSchemaAsync(int ver);

    /**
     * Returns a schema corresponding to the latest Catalog version, which is available on node locally.
     * Note: For test purposes only. Method does NOT wait for actual Catalog version in the grid.
     */
    @TestOnly
    SchemaPlus latestSchema(@Nullable String name);
}
