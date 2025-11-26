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
import static java.util.Collections.unmodifiableNavigableMap;

import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Immutable index meta, based on the {@link CatalogIndexDescriptor}. */
public class IndexMeta {
    private final int catalogVersion;

    private final int indexId;

    private final int tableId;

    private final int tableVersionOnIndexCreation;

    private final String indexName;

    private final MetaIndexStatus currentStatus;

    @IgniteToStringInclude
    private final Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges;

    private final NavigableMap<Long, MetaIndexStatus> statusActivationTimeline;

    /**
     * Constructor.
     *
     * @param catalogVersion Catalog version in which the current meta was created.
     * @param indexId Index ID.
     * @param tableId Table ID to which the index belongs.
     * @param tableVersion Version of the table at the time the index was created in the catalog.
     * @param indexName Index name.
     * @param currentStatus Current status of the index
     * @param statusChanges <b>Immutable</b> map of index statuses with change info (for example catalog version) in which they appeared.
     */
    IndexMeta(
            int catalogVersion,
            int indexId,
            int tableId,
            int tableVersion,
            String indexName,
            MetaIndexStatus currentStatus,
            Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges
    ) {
        this.catalogVersion = catalogVersion;
        this.indexId = indexId;
        this.tableId = tableId;
        this.tableVersionOnIndexCreation = tableVersion;
        this.indexName = indexName;
        this.currentStatus = currentStatus;
        this.statusChanges = unmodifiableMap(statusChanges);

        NavigableMap<Long, MetaIndexStatus> timeline = new TreeMap<>();
        for (Entry<MetaIndexStatus, MetaIndexStatusChange> entry : statusChanges.entrySet()) {
            timeline.put(entry.getValue().activationTimestamp(), entry.getKey());
        }
        this.statusActivationTimeline = unmodifiableNavigableMap(timeline);
    }

    /**
     * Creates a index meta instance.
     *
     * @param indexId Catalog index ID.
     * @param catalog Catalog version from which the {@code catalogIndexDescriptor} was taken.
     */
    static IndexMeta of(int indexId, Catalog catalog) {
        CatalogIndexDescriptor catalogIndexDescriptor = catalog.index(indexId);

        assert catalogIndexDescriptor != null : "indexId=" + indexId + ", catalogVersion=" + catalog.version();

        CatalogTableDescriptor catalogTableDescriptor = catalog.table(catalogIndexDescriptor.tableId());

        assert catalogTableDescriptor != null :
                "indexId=" + indexId + ", tableId=" + catalogIndexDescriptor.tableId() + ", catalogVersion=" + catalog.version();

        return new IndexMeta(
                catalog.version(),
                catalogIndexDescriptor.id(),
                catalogIndexDescriptor.tableId(),
                catalogTableDescriptor.latestSchemaVersion(),
                catalogIndexDescriptor.name(),
                MetaIndexStatus.convert(catalogIndexDescriptor.status()),
                Map.of(
                        MetaIndexStatus.convert(catalogIndexDescriptor.status()),
                        new MetaIndexStatusChange(catalog.version(), catalog.time())
                )
        );
    }

    /** Returns catalog version in which the current meta was created. */
    public int catalogVersion() {
        return catalogVersion;
    }

    /** Returns index ID. */
    public int indexId() {
        return indexId;
    }

    /** Returns table ID to which the index belongs. */
    public int tableId() {
        return tableId;
    }

    /** Returns version of the table at the time the index was created in the catalog. */
    public int tableVersion() {
        return tableVersionOnIndexCreation;
    }

    /** Returns index name. */
    public String indexName() {
        return indexName;
    }

    /**
     * Changes the index name.
     *
     * @param catalogVersion Catalog version in which the index name has changed.
     * @param newIndexName New index name.
     * @return New instance of the index meta with only a new index name.
     */
    IndexMeta indexName(int catalogVersion, String newIndexName) {
        return new IndexMeta(
                catalogVersion,
                indexId,
                tableId,
                tableVersionOnIndexCreation,
                newIndexName,
                currentStatus,
                new EnumMap<>(statusChanges)
        );
    }

    /** Returns the current status of the index. */
    public MetaIndexStatus status() {
        return currentStatus;
    }

    /**
     * Sets the new current index status and adds to {@link #statusChanges()}.
     *
     * @param newStatus New current status of the index.
     * @param catalogVersion Catalog version in which the new index status appeared.
     * @param activationTs Activation timestamp of the catalog version in which the new status appeared.
     * @return New instance of the index meta with a change in the current status and status history updates.
     * @see Catalog#time()
     */
    IndexMeta status(MetaIndexStatus newStatus, int catalogVersion, long activationTs) {
        assert !statusChanges.containsKey(newStatus) :
                String.format("newStatus=%s, catalogVersion=%d, indexName=%s", newStatus, catalogVersion, indexName);

        var newStatuses = new EnumMap<>(statusChanges);
        newStatuses.put(newStatus, new MetaIndexStatusChange(catalogVersion, activationTs));

        return new IndexMeta(
                catalogVersion,
                indexId,
                tableId,
                tableVersionOnIndexCreation,
                indexName,
                newStatus,
                newStatuses
        );
    }

    public @Nullable MetaIndexStatus statusAt(long timestamp) {
        Entry<Long, MetaIndexStatus> floorEntry = statusActivationTimeline.floorEntry(timestamp);
        return floorEntry != null ? floorEntry.getValue() : null;
    }

    /** Returns a map of index statuses with change info (for example catalog version) in which they appeared. */
    public Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges() {
        return statusChanges;
    }

    /**
     * Returns the index status change.
     *
     * @param status Index status of interest.
     * @throws IllegalArgumentException If there is no index status change.
     */
    public MetaIndexStatusChange statusChange(MetaIndexStatus status) {
        MetaIndexStatusChange change = statusChanges.get(status);

        if (change == null) {
            throw new IllegalArgumentException(String.format("No status change: [requestedStatus=%s, indexMeta=%s]", status, this));
        }

        return change;
    }

    /** Returns the index status change, {@code null} if absent. */
    public @Nullable MetaIndexStatusChange statusChangeNullable(MetaIndexStatus status) {
        return statusChanges.get(status);
    }

    /** Returns {@code true} if the index was dropped. */
    public boolean isDropped() {
        switch (currentStatus) {
            case STOPPING:
            case REMOVED:
            case READ_ONLY:
                return true;
            case REGISTERED:
            case BUILDING:
            case AVAILABLE:
                return false;
            default:
                throw new AssertionError(String.format("Unknown status: [indexId=%s, currentStatus=%s]", indexId, currentStatus));
        }
    }

    /** Returns {@code true} if the index was already removed from the Catalog (it can still exist and function though). */
    public boolean isRemovedFromCatalog() {
        switch (currentStatus) {
            case REMOVED:
            case READ_ONLY:
                return true;
            case REGISTERED:
            case BUILDING:
            case AVAILABLE:
            case STOPPING:
                return false;
            default:
                throw new AssertionError(String.format("Unknown status: [indexId=%s, currentStatus=%s]", indexId, currentStatus));
        }
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
