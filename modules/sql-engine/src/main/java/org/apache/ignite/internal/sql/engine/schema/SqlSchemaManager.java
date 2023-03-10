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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.schema.SchemaPlus;
import org.jetbrains.annotations.Nullable;

/**
 * Sql schemas operations interface.
 */
public interface SqlSchemaManager {
    /**
     * Returns a required schema if specified, or default schema otherwise.
     */
    SchemaPlus schema(@Nullable String schema);

    /**
     * Returns a table by given id.
     *
     * @param id An id of required table.
     *
     * @return The table.
     */
    IgniteTable tableById(UUID id);

    /**
     * Wait for {@code ver} schema version, just a stub, need to be removed after IGNITE-18733.
     */
    CompletableFuture<?> waitActualSchema(long ver);

    /**
     * Return last applied schema version, just a stub, need to be removed after IGNITE-18733.
     *
     * @return Current version.
     */
    long lastAppliedVersion(String schema);
}
