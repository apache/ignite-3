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

package org.apache.ignite.internal.table.distributed.index;

import static java.util.Collections.unmodifiableMap;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.Map;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Immutable index meta, based on the {@link CatalogIndexDescriptor}, tracks its changes and is stored in vault.
 *
 * <p>Unlike the {@link CatalogIndexDescriptor}, the index meta is not affected by catalog compaction and will store the necessary
 * data until the index is destroyed under a {@link LowWatermark}.</p>
 */
public class IndexMeta implements Serializable {
    private static final long serialVersionUID = 1044129530453957897L;

    private final int indexId;

    private final int tableId;

    private final String indexName;

    private final MetaIndexStatusEnum status;

    @IgniteToStringInclude
    private final Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> statuses;

    /** Constructor. */
    IndexMeta(CatalogIndexDescriptor catalogIndexDescriptor, Catalog catalog) {
        this(
                catalogIndexDescriptor.id(),
                catalogIndexDescriptor.tableId(),
                catalogIndexDescriptor.name(),
                MetaIndexStatusEnum.convert(catalogIndexDescriptor.status()),
                Map.of(
                        MetaIndexStatusEnum.convert(catalogIndexDescriptor.status()),
                        new MetaIndexStatusChangeInfo(catalog.version(), catalog.time())
                )
        );
    }

    /** Constructor. */
    private IndexMeta(
            int indexId,
            int tableId,
            String indexName,
            MetaIndexStatusEnum status,
            Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> statuses
    ) {
        this.indexId = indexId;
        this.tableId = tableId;
        this.indexName = indexName;
        this.status = status;
        this.statuses = unmodifiableMap(statuses);
    }

    /** Returns index ID. */
    public int indexId() {
        return indexId;
    }

    /** Returns table ID to which the index belongs. */
    public int tableId() {
        return tableId;
    }

    /** Returns index name. */
    public String indexName() {
        return indexName;
    }

    /**
     * Changes the index name.
     *
     * @param newIndexName New index name.
     * @return New instance of the index meta.
     */
    IndexMeta indexName(String newIndexName) {
        return new IndexMeta(indexId, tableId, newIndexName, status, new EnumMap<>(statuses));
    }

    /** Returns the current status of the index. */
    public MetaIndexStatusEnum status() {
        return status;
    }

    /**
     * Sets the new current index status and adds to {@link #statuses()}.
     *
     * @param newStatus New current status of the index.
     * @param catalogVersion Catalog version in which the new index status appeared.
     * @param activationTs Activation timestamp of the catalog version in which the new status appeared.
     * @return New instance of the index meta.
     * @see Catalog#time()
     */
    IndexMeta status(MetaIndexStatusEnum newStatus, int catalogVersion, long activationTs) {
        assert !statuses.containsKey(newStatus) : "newStatus=" + newStatus + ", catalogVersion=" + catalogVersion;

        var newStatuses = new EnumMap<>(statuses);
        newStatuses.put(newStatus, new MetaIndexStatusChangeInfo(catalogVersion, activationTs));

        return new IndexMeta(indexId, tableId, indexName, newStatus, newStatuses);
    }

    /** Returns a map of index statuses with change info (for example catalog version) in which they appeared. */
    public Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> statuses() {
        return statuses;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
