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

package org.apache.ignite.internal.cli.sql;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SQL schema provider.
 */
public class SqlSchemaProvider implements SchemaProvider {
    private static final int SCHEMA_UPDATE_TIMEOUT = 10;

    private final SqlSchemaLoader sqlSchemaLoader;

    private final int schemaUpdateTimeout;

    private final AtomicReference<SqlSchema> schema = new AtomicReference<>(null);

    private final AtomicReference<Instant> lastUpdate = new AtomicReference<>(Instant.now());

    public SqlSchemaProvider(MetadataSupplier metadataSupplier) {
        this(metadataSupplier, SCHEMA_UPDATE_TIMEOUT);
    }

    SqlSchemaProvider(MetadataSupplier metadataSupplier, int schemaUpdateTimeout) {
        sqlSchemaLoader = new SqlSchemaLoader(metadataSupplier);
        this.schemaUpdateTimeout = schemaUpdateTimeout;
    }

    @Override
    public SqlSchema getSchema() {
        if (schema.compareAndSet(null, sqlSchemaLoader.loadSchema())) {
            lastUpdate.set(Instant.now());
            return schema.get();
        } else if (Duration.between(lastUpdate.get(), Instant.now()).toSeconds() >= schemaUpdateTimeout) {
            CompletableFuture.supplyAsync(() -> {
                schema.set(sqlSchemaLoader.loadSchema());
                lastUpdate.set(Instant.now());
                return schema.get();
            });
        }

        return schema.get();
    }

    public void initStateAsync() {
        // trigger schema loading in background
        CompletableFuture.supplyAsync(this::getSchema);
    }
}
