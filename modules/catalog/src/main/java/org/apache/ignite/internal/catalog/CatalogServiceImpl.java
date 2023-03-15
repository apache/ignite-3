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

package org.apache.ignite.internal.catalog;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.catalog.descriptors.CatalogDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.manager.Producer;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: IGNITE-18535 Fix javadoc
 */
public class CatalogServiceImpl extends Producer<CatalogEvent, CatalogEventParameters> implements CatalogService {

    /** Versioned catalog descriptors. */
    private final ConcurrentMap<Integer, CatalogDescriptor> catalogByVer = new ConcurrentHashMap<>(); //TODO: IGNITE-18535 Use IntMap instead.

    /** Versioned catalog descriptors sorted in chronological order. */
    private final ConcurrentNavigableMap<Long, CatalogDescriptor> catalogByTs = new ConcurrentSkipListMap<>(); //TODO: IGNITE-18535 Use LongMap instead.

    /** {@inheritDoc} */
    @Override
    public TableDescriptor table(String tableName, long timestamp) {
        return catalogAt(timestamp).schema(CatalogDescriptor.DEFAULT_SCHEMA_NAME).table(tableName);
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor table(int tableId, long timestamp) {
        return catalogAt(timestamp).table(tableId);
    }

    /** {@inheritDoc} */
    @Override
    public IndexDescriptor index(int indexId, long timestamp) {
        return catalogAt(timestamp).index(indexId);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<IndexDescriptor> tableIndexes(int tableId, long timestamp) {
        return catalogAt(timestamp).tableIndexes(tableId);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaDescriptor schema(int version) {
        return catalog(version).schema(CatalogDescriptor.DEFAULT_SCHEMA_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor activeSchema(long timestamp) {
        return catalogAt(timestamp).schema(CatalogDescriptor.DEFAULT_SCHEMA_NAME);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<SchemaDescriptor> updateSchema(DynamicConfigurationChanger changer) {
        return null;
    }

    private CatalogDescriptor catalog(int version) {
        return catalogByVer.get(version);
    }

    private CatalogDescriptor catalogAt(long timestamp) {
        Entry<Long, CatalogDescriptor> entry = catalogByTs.floorEntry(timestamp);

        if (entry == null) {
            throw new IllegalStateException("No valid schema found for given timestamp: " + timestamp);
        }

        return entry.getValue();
    }
}
