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

package org.apache.ignite.internal.table.distributed.schema;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSyncService;

/**
 * Provides access to table schemas.
 */
public interface ValidationSchemasSource {
    /**
     * Obtains a future that completes when the given schema version becomes available.
     *
     * <p>Must only be called when it's guaranteed that the table exists from the point of view of {@link SchemaManager}.
     *
     * @param tableId ID of the table of interest.
     * @param schemaVersion ID of the schema version.
     * @return Future that completes when the given schema version becomes available.
     */
    CompletableFuture<Void> waitForSchemaAvailability(int tableId, int schemaVersion);

    /**
     * Returns all schema versions between (including) the two that were effective at the given timestamps.
     *
     * <p>For both timestamps, schemas-related metadata must be complete, see {@link SchemaSyncService}.
     *
     * @param tableId ID of the table which schemas need to be considered.
     * @param fromIncluding Start timestamp.
     * @param toIncluding End timestamp.
     * @return All schema versions between (including) the two that were effective at the given timestamps.
     */
    List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding);

    /**
     * Returns all schema versions between (including) the one that was effective at the given timestamp and
     * the one identified by a schema version ID. If the starting schema (the one effective at fromIncluding)
     * is actually a later schema than the one identified by toIncluding, then an empty list is returned.
     *
     * <p>For both fromIncluding and toIncluding, schemas-related metadata must be complete.
     *
     * @param tableId ID of the table which schemas need to be considered.
     * @param fromIncluding Start timestamp.
     * @param toTableVersionIncluding End schema version ID.
     * @return All schema versions between (including) the given timestamp and schema version.
     */
    List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, int toTableVersionIncluding);
}
