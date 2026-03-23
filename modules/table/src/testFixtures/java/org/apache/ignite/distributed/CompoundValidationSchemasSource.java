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

package org.apache.ignite.distributed;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.schema.FullTableSchema;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;

class CompoundValidationSchemasSource implements ValidationSchemasSource {
    private final Map<Integer, ValidationSchemasSource> schemasSources = new ConcurrentHashMap<>();

    void registerSource(int table, ValidationSchemasSource source) {
        schemasSources.put(table, source);
    }

    @Override
    public CompletableFuture<Void> waitForSchemaAvailability(int tableId, int schemaVersion) {
        return requiredSourceForTable(tableId).waitForSchemaAvailability(tableId, schemaVersion);
    }

    private ValidationSchemasSource requiredSourceForTable(int tableId) {
        return requireNonNull(schemasSources.get(tableId), "No source for table " + tableId);
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        return requiredSourceForTable(tableId).tableSchemaVersionsBetween(tableId, fromIncluding, toIncluding);
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, int toTableVersionIncluding) {
        return requiredSourceForTable(tableId).tableSchemaVersionsBetween(tableId, fromIncluding, toTableVersionIncluding);
    }
}
