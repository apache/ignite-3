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

package org.apache.ignite.internal.index;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.ChangeIndexStatusValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;

/** Helper class for index management. */
class IndexManagementUtils {
    /** Metastore key prefix for the "index in the process of building" in the format: {@code "indexBuild.inProgress.<indexId>"}. */
    static final String IN_PROGRESS_BUILD_INDEX_KEY_PREFIX = "indexBuild.inProgress.";

    /**
     * Metastore key prefix for the "index in the process of building for partition" in the format:
     * {@code "indexBuild.partition.<indexId>.<partitionId>"}.
     */
    static final String PARTITION_BUILD_INDEX_KEY_PREFIX = "indexBuild.partition.";

    /** Timeout to receive the primary replica meta in seconds. */
    static final long AWAIT_PRIMARY_REPLICA_TIMEOUT_SEC = 10;

    /**
     * Returns {@code true} if the {@code key} is <b>absent</b> in the metastore locally.
     *
     * @param metastore Metastore manager.
     * @param key Key to check.
     * @param revUpperBound Upper bound of metastore revision.
     */
    static boolean isMetastoreKeyAbsentLocally(MetaStorageManager metastore, ByteArray key, long revUpperBound) {
        return metastore.getLocally(key, revUpperBound).value() == null;
    }

    /**
     * Returns {@code true} if at least one key by prefix is <b>present</b> in the metastore locally.
     *
     * @param metastore Metastore manager.
     * @param keyPrefix Key prefix to check.
     * @param revUpperBound Upper bound of metastore revision.
     */
    static boolean isAnyMetastoreKeyPresentLocally(MetaStorageManager metastore, ByteArray keyPrefix, long revUpperBound) {
        try (Cursor<Entry> cursor = metastore.prefixLocally(keyPrefix, revUpperBound)) {
            return cursor.stream().map(Entry::value).anyMatch(Objects::nonNull);
        }
    }

    /**
     * Removes a {@code key} from the metastore if <b>present</b>.
     *
     * @param metaStorageManager Metastore manager.
     * @param key Key to remove.
     * @return Future result {@code true} if actual removal happened, otherwise {@code false}.
     */
    static CompletableFuture<Boolean> removeMetastoreKeyIfPresent(MetaStorageManager metaStorageManager, ByteArray key) {
        return metaStorageManager.invoke(exists(key), remove(key), noop());
    }

    /**
     * Puts index building keys into the metastore if they are absent.
     *
     * <p>NOTES: Presence of keys is determined by {@value #IN_PROGRESS_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>"}.</p>
     *
     * <p>Keys: </p>
     * <ul>
     *     <li>{@value #IN_PROGRESS_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>"}.</li>
     *     <li>{@value #PARTITION_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>.0"} ...
     *     {@value #PARTITION_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>.<partitions-1>"}.</li>
     * </ul>
     *
     * @param metastore Metastore manager.
     * @param indexId Index ID.
     * @param partitions Partition count.
     * @return Future result {@code true} if success update was applied, otherwise {@code false}.
     */
    static CompletableFuture<Boolean> putBuildIndexMetastoreKeysIfAbsent(MetaStorageManager metastore, int indexId, int partitions) {
        ByteArray inProgressBuildIndexMetastoreKey = inProgressBuildIndexMetastoreKey(indexId);

        List<Operation> putPartitionBuildIndexMetastoreKeyOperations = IntStream.range(0, partitions)
                .mapToObj(partitionId -> put(partitionBuildIndexMetastoreKey(indexId, partitionId), BYTE_EMPTY_ARRAY))
                .collect(toList());

        return metastore.invoke(
                notExists(inProgressBuildIndexMetastoreKey),
                concat(
                        List.of(put(inProgressBuildIndexMetastoreKey, BYTE_EMPTY_ARRAY)),
                        putPartitionBuildIndexMetastoreKeyOperations
                ),
                List.of(noop())
        );
    }

    /**
     * Returns the "index in the process of building" metastore key, format:
     * {@value #IN_PROGRESS_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>"}.
     *
     * @param indexId Index ID.
     */
    static ByteArray inProgressBuildIndexMetastoreKey(int indexId) {
        return ByteArray.fromString(IN_PROGRESS_BUILD_INDEX_KEY_PREFIX + indexId);
    }

    /**
     * Returns the "building an index for the partition" metastore prefix key, format:
     * {@value #PARTITION_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>"}.
     *
     * @param indexId Index ID.
     */
    static ByteArray partitionBuildIndexMetastoreKeyPrefix(int indexId) {
        return ByteArray.fromString(PARTITION_BUILD_INDEX_KEY_PREFIX + indexId);
    }

    /**
     * Returns the "building an index for the partition" metastore key, format:
     * {@value #PARTITION_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>.<partitionId>"}.
     *
     * @param indexId Index ID.
     * @param partitionId Partition ID.
     */
    static ByteArray partitionBuildIndexMetastoreKey(int indexId, int partitionId) {
        return ByteArray.fromString(PARTITION_BUILD_INDEX_KEY_PREFIX + indexId + '.' + partitionId);
    }

    /**
     * Converts bytes to string key: {@value PARTITION_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>.<partitionId>"}.
     *
     * @param bytes Bytes to convert.
     */
    static String toPartitionBuildIndexMetastoreKeyString(byte[] bytes) {
        String keyStr = new String(bytes, UTF_8);

        assert keyStr.startsWith(PARTITION_BUILD_INDEX_KEY_PREFIX) : keyStr;

        return keyStr;
    }

    /**
     * Returns partition count from the catalog.
     *
     * @param catalogService Catalog service.
     * @param indexId Index ID.
     * @param catalogVersion Catalog version.
     */
    static int getPartitionCountFromCatalog(CatalogService catalogService, int indexId, int catalogVersion) {
        CatalogIndexDescriptor indexDescriptor = index(catalogService, indexId, catalogVersion);

        CatalogTableDescriptor tableDescriptor = catalogService.table(indexDescriptor.tableId(), catalogVersion);

        assert tableDescriptor != null : "tableId=" + indexDescriptor.tableId() + ", catalogVersion=" + catalogVersion;

        CatalogZoneDescriptor zoneDescriptor = catalogService.zone(tableDescriptor.zoneId(), catalogVersion);

        assert zoneDescriptor != null : "zoneId=" + tableDescriptor.zoneId() + ", catalogVersion=" + catalogVersion;

        return zoneDescriptor.partitions();
    }

    /**
     * Finds an index by ID in the requested catalog version. Throws if it does not exist.
     *
     * @param catalogService Catalog service to be used to find the index.
     * @param indexId ID of the index to find.
     * @param catalogVersion Version of the catalog in which to look for the index.
     */
    static CatalogIndexDescriptor index(CatalogService catalogService, int indexId, int catalogVersion) {
        CatalogIndexDescriptor indexDescriptor = catalogService.index(indexId, catalogVersion);

        assert indexDescriptor != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;
        return indexDescriptor;
    }

    /**
     * Makes the index available in the catalog, does not return the future execution of the operation, so as not to create dead locks when
     * performing the operation and the inability to complete it due to execution in the metastore thread or on recovery (the metastore
     * watches will not be deployed yet). Logs errors if it is not {@link IndexNotFoundValidationException},
     * {@link ChangeIndexStatusValidationException} or {@link NodeStoppingException}.
     *
     * @param catalogManager Catalog manger.
     * @param indexId Index ID.
     * @param log Logger.
     */
    static void makeIndexAvailableInCatalogWithoutFuture(CatalogManager catalogManager, int indexId, IgniteLogger log) {
        catalogManager
                .execute(MakeIndexAvailableCommand.builder().indexId(indexId).build())
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        Throwable unwrapCause = unwrapCause(throwable);

                        if (!(unwrapCause instanceof IndexNotFoundValidationException)
                                && !(unwrapCause instanceof ChangeIndexStatusValidationException)
                                && !(unwrapCause instanceof NodeStoppingException)) {
                            log.error("Error processing the command to make the index available: {}", unwrapCause, indexId);
                        }
                    }
                });
    }

    /**
     * Extracts a partition ID from the key: {@value PARTITION_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>.<partitionId>"}.
     *
     * @param key Key.
     * @return Partition ID.
     */
    static int extractPartitionIdFromPartitionBuildIndexKey(String key) {
        assert key.startsWith(PARTITION_BUILD_INDEX_KEY_PREFIX) : key;

        String[] strings = key.split("\\.");

        return Integer.parseInt(strings[3]);
    }

    /**
     * Extracts a index ID from the key: {@value PARTITION_BUILD_INDEX_KEY_PREFIX} + {@code "<indexId>.<partitionId>"}.
     *
     * @param key Key.
     * @return Index ID.
     */
    static int extractIndexIdFromPartitionBuildIndexKey(String key) {
        assert key.startsWith(PARTITION_BUILD_INDEX_KEY_PREFIX) : key;

        String[] strings = key.split("\\.");

        return Integer.parseInt(strings[2]);
    }

    /**
     * Returns {@code true} if the local node is the primary replica at the timestamp of interest.
     *
     * @param primaryReplicaMeta Primary replica meta.
     * @param localNode Local node.
     * @param timestamp Timestamp of interest.
     */
    static boolean isPrimaryReplica(ReplicaMeta primaryReplicaMeta, ClusterNode localNode, HybridTimestamp timestamp) {
        return localNode.id().equals(primaryReplicaMeta.getLeaseholderId())
                && timestamp.compareTo(primaryReplicaMeta.getExpirationTime()) < 0;
    }

    /**
     * Returns the local node.
     *
     * @param clusterService Cluster service.
     */
    static ClusterNode localNode(ClusterService clusterService) {
        return clusterService.topologyService().localMember();
    }

    /**
     * Returns {@code true} if the passed node ID is equal to the local node ID, {@code false} otherwise.
     *
     * @param clusterService Cluster service.
     * @param nodeId Node ID of interest.
     */
    static boolean isLocalNode(ClusterService clusterService, String nodeId) {
        return nodeId.equals(localNode(clusterService).id());
    }

    /**
     * Enters "busy" state for two locks.
     *
     * <p>NOTE: Then you should {@link IndexManagementUtils#leaveBusy(IgniteSpinBusyLock, IgniteSpinBusyLock)} with the same order of
     * locks.</p>
     *
     * @return {@code true} if entered to busy state.
     */
    static boolean enterBusy(IgniteSpinBusyLock busyLock0, IgniteSpinBusyLock busyLock1) {
        if (!busyLock0.enterBusy()) {
            return false;
        }

        if (!busyLock1.enterBusy()) {
            busyLock0.leaveBusy();

            return false;
        }

        return true;
    }

    /**
     * Leaves "busy" state for two locks.
     *
     * <p>NOTE: Before this you need to {@link IndexManagementUtils#enterBusy(IgniteSpinBusyLock, IgniteSpinBusyLock)} with the same order
     * of locks.</p>
     */
    static void leaveBusy(IgniteSpinBusyLock busyLock0, IgniteSpinBusyLock busyLock1) {
        busyLock1.leaveBusy();
        busyLock0.leaveBusy();
    }
}
