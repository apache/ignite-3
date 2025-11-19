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

package org.apache.ignite.internal.table.distributed;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.assignmentsChainKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.tableAssignmentsGetLocally;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_INDEX;
import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_TERM;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngine;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsChain;
import org.apache.ignite.internal.replicator.TablePartitionId;

/** Manages table partitions assignments (excluding rebalance, see {@link DistributionZoneRebalanceEngine}). */
public class TableAssignmentsService {
    private static final IgniteLogger LOG = Loggers.forClass(TableAssignmentsService.class);

    private final MetaStorageManager metaStorageMgr;
    private final CatalogService catalogService;
    private final DistributionZoneManager distributionZoneManager;
    private final FailureProcessor failureProcessor;

    /** Constructor. */
    public TableAssignmentsService(
            MetaStorageManager metaStorageMgr,
            CatalogService catalogService,
            DistributionZoneManager distributionZoneManager,
            FailureProcessor failureProcessor
    ) {
        this.metaStorageMgr = metaStorageMgr;
        this.catalogService = catalogService;
        this.distributionZoneManager = distributionZoneManager;
        this.failureProcessor = failureProcessor;
    }

    CompletableFuture<List<Assignments>> createAndWriteTableAssignmentsToMetastorage(
            int tableId,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogTableDescriptor tableDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        CompletableFuture<List<Assignments>> assignments =
                getOrCreateAssignments(tableId, zoneDescriptor, tableDescriptor, causalityToken, catalogVersion);

        return writeTableAssignmentsToMetastore(tableId, zoneDescriptor.consistencyMode(), assignments);
    }

    /**
     * Writes the set of assignments to meta storage. If there are some assignments already, gets them from meta storage. Returns the list
     * of assignments that really are in meta storage.
     *
     * @return Real list of assignments.
     */
    public CompletableFuture<List<Assignments>> writeTableAssignmentsToMetastore(
            int tableId,
            ConsistencyMode consistencyMode,
            CompletableFuture<List<Assignments>> assignmentsFuture
    ) {
        return assignmentsFuture.thenCompose(newAssignments -> {
            assert !newAssignments.isEmpty();

            List<Operation> partitionAssignments = getTableAssignmentsOperations(tableId, newAssignments, consistencyMode);

            Condition condition = notExists(new ByteArray(toByteArray(partitionAssignments.get(0).key())));

            return metaStorageMgr
                    .invoke(condition, partitionAssignments, Collections.emptyList())
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            String errorMessage = String.format(
                                    "Couldn't write assignments [assignmentsList=%s] to metastore during invoke.",
                                    Assignments.assignmentListToString(newAssignments)
                            );
                            failureProcessor.process(new FailureContext(e, errorMessage));
                        }
                    })
                    .thenCompose(invokeResult -> {
                        if (invokeResult) {
                            LOG.info(
                                    "Assignments calculated from data nodes are successfully written to meta storage"
                                            + " [tableId={}, assignments={}].",
                                    tableId,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            return completedFuture(newAssignments);
                        } else {
                            return getAssignmentsFromMetastorage(tableId, newAssignments.size());
                        }
                    });
        });
    }

    /**
     * Check if the table already has assignments in the meta storage locally. So, it means, that it is a recovery process and we should use
     * the meta storage local assignments instead of calculation of the new ones.
     */
    private CompletableFuture<List<Assignments>> getOrCreateAssignments(
            int tableId,
            CatalogZoneDescriptor zoneDescriptor,
            CatalogTableDescriptor tableDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        CompletableFuture<List<Assignments>> assignmentsFuture;

        if (partitionAssignmentsGetLocally(metaStorageMgr, tableId, 0, causalityToken) != null) {
            assignmentsFuture = completedFuture(
                    tableAssignmentsGetLocally(metaStorageMgr, tableId, zoneDescriptor.partitions(), causalityToken));
        } else {
            Catalog catalog = catalogService.catalog(catalogVersion);

            long assignmentsTimestamp = catalog.time();

            assignmentsFuture = distributionZoneManager.dataNodes(catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes -> calculateAssignments(
                                    dataNodes,
                                    zoneDescriptor.partitions(),
                                    zoneDescriptor.replicas(),
                                    zoneDescriptor.consensusGroupSize()
                            )
                                    .stream()
                                    .map(assignments -> Assignments.of(assignments, assignmentsTimestamp))
                                    .collect(toList())
                    );

            assignmentsFuture.thenAccept(assignmentsList -> LOG.info(
                    "Assignments calculated from data nodes [tableId={}, assignments={}, revision={}]",
                    tableId,
                    Assignments.assignmentListToString(assignmentsList),
                    causalityToken
            ));
        }

        return assignmentsFuture;
    }

    private CompletableFuture<List<Assignments>> getAssignmentsFromMetastorage(int tableId, int partitions) {
        Set<ByteArray> partKeys = IntStream.range(0, partitions)
                .mapToObj(p -> stablePartAssignmentsKey(new TablePartitionId(tableId, p)))
                .collect(toSet());

        CompletableFuture<Map<ByteArray, Entry>> assignmentsFuture = metaStorageMgr.getAll(partKeys);

        return assignmentsFuture.thenApply(metaStorageAssignments -> {
            List<Assignments> realAssignments = new ArrayList<>();

            for (int p = 0; p < partitions; p++) {
                var partId = new TablePartitionId(tableId, p);
                Entry assignmentsEntry = metaStorageAssignments.get(stablePartAssignmentsKey(partId));
                assert assignmentsEntry != null && !assignmentsEntry.empty() && !assignmentsEntry.tombstone()
                        : "Unexpected assignments for partition [" + partId + ", entry=" + assignmentsEntry + "].";
                Assignments real = Assignments.fromBytes(assignmentsEntry.value());
                realAssignments.add(real);
            }

            LOG.info(
                    "Assignments picked up from meta storage [tableId={}, assignments={}].",
                    tableId,
                    Assignments.assignmentListToString(realAssignments)
            );

            return realAssignments;
        }).whenComplete((realAssignments, e) -> {
            if (e != null) {
                String errorMessage = String.format("Couldn't get assignments from metastore for table [tableId=%s].", tableId);
                failureProcessor.process(new FailureContext(e, errorMessage));
            }
        });
    }

    private static List<Operation> getTableAssignmentsOperations(
            int tableId,
            List<Assignments> assignments,
            ConsistencyMode consistencyMode
    ) {
        boolean haMode = consistencyMode == ConsistencyMode.HIGH_AVAILABILITY;

        var partitionAssignments = new ArrayList<Operation>(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, i);

            ByteArray stableAssignmentsKey = stablePartAssignmentsKey(tablePartitionId);
            byte[] anAssignment = assignments.get(i).toBytes();
            Operation op = put(stableAssignmentsKey, anAssignment);
            partitionAssignments.add(op);

            if (haMode) {
                ByteArray assignmentsChainKey = assignmentsChainKey(tablePartitionId);
                byte[] assignmentChain = AssignmentsChain.of(UNKNOWN_TERM, UNKNOWN_INDEX, assignments.get(i)).toBytes();
                Operation chainOp = put(assignmentsChainKey, assignmentChain);
                partitionAssignments.add(chainOp);
            }
        }

        return partitionAssignments;
    }
}
