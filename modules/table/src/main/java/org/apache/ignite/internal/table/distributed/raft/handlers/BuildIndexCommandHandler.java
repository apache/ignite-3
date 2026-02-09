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

package org.apache.ignite.internal.table.distributed.raft.handlers;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_APPLIED_RESULT;
import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_NOT_APPLIED_RESULT;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.util.CollectionUtils.last;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommandV3;
import org.apache.ignite.internal.partition.replicator.raft.CommandResult;
import org.apache.ignite.internal.partition.replicator.raft.handlers.AbstractCommandHandler;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowUpgrader;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.jetbrains.annotations.Nullable;

/**
 * Raft command handler that handles {@link BuildIndexCommandV2}.
 *
 * <p>We will also handle {@link BuildIndexCommand}, since there is no specific logic for {@link BuildIndexCommandV2}, we will leave it
 * as is to support backward compatibility.</p>
 */
public class BuildIndexCommandHandler extends AbstractCommandHandler<BuildIndexCommand> {
    private static final IgniteLogger LOG = Loggers.forClass(BuildIndexCommandHandler.class);

    /** Data storage to which the command will be applied. */
    private final PartitionDataStorage storage;

    private final IndexMetaStorage indexMetaStorage;

    /** Handler that processes storage updates. */
    private final StorageUpdateHandler storageUpdateHandler;

    private final SchemaRegistry schemaRegistry;

    /**
     * Creates a new instance of the command handler.
     *
     * @param storage Partition data storage.
     * @param indexMetaStorage Index meta storage.
     * @param storageUpdateHandler Storage update handler.
     * @param schemaRegistry Schema registry.
     */
    public BuildIndexCommandHandler(
            PartitionDataStorage storage,
            IndexMetaStorage indexMetaStorage,
            StorageUpdateHandler storageUpdateHandler,
            SchemaRegistry schemaRegistry
    ) {
        this.storage = storage;
        this.indexMetaStorage = indexMetaStorage;
        this.storageUpdateHandler = storageUpdateHandler;
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    protected CommandResult handleInternally(
            BuildIndexCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) throws IgniteInternalException {
        // Skips the write command because the storage has already executed it.
        if (commandIndex <= storage.lastAppliedIndex()) {
            return EMPTY_NOT_APPLIED_RESULT;
        }

        IndexMeta indexMeta = indexMetaStorage.indexMeta(command.indexId());

        if (indexMeta == null || indexMeta.isDropped()) {
            // Index has been dropped.
            return EMPTY_APPLIED_RESULT;
        }

        Set<UUID> abortedTransactionIds = command instanceof BuildIndexCommandV3
                ? ((BuildIndexCommandV3) command).abortedTransactionIds()
                : Set.of();
        BuildIndexRowVersionChooser rowVersionChooser = createBuildIndexRowVersionChooser(indexMeta, abortedTransactionIds);

        BinaryRowUpgrader binaryRowUpgrader = createBinaryRowUpgrader(indexMeta);

        List<RowId> rowIds = command.rowIds().stream()
                // Natural UUID order matches RowId order within the same partition.
                .sorted()
                .map(this::toRowId)
                .collect(Collectors.toList());
        @Nullable RowId lastRowId = last(rowIds);

        AtomicInteger rowIdsIterationIndex = new AtomicInteger(0);
        boolean finished = false;
        while (!finished) {
            finished = storage.runConsistently(locker -> {
                if (rowIds.isEmpty()) {
                    // necessary to bump `nextRowIdToBuild` to null
                    storageUpdateHandler.getIndexUpdateHandler().buildIndex(command.indexId(), Stream.of(), null);
                } else {
                    int index = rowIdsIterationIndex.get();
                    while (index < rowIds.size()) {
                        RowId rowId = rowIds.get(index);
                        locker.lock(rowId);
                        Stream<BinaryRowAndRowId> rowVersions = rowVersionChooser.chooseForBuildIndex(rowId)
                                .stream()
                                .map(row -> upgradeBinaryRow(binaryRowUpgrader, row));

                        RowId nextRowIdToBuild = null;
                        if (index != rowIds.size() - 1) {
                            nextRowIdToBuild = rowIds.get(index + 1);
                        } else if (!command.finish()) {
                            nextRowIdToBuild = requireNonNull(lastRowId).increment();
                        }

                        storageUpdateHandler.getIndexUpdateHandler().buildIndex(command.indexId(), rowVersions, nextRowIdToBuild);
                        index = rowIdsIterationIndex.incrementAndGet();

                        if (locker.shouldRelease() && index < rowIds.size()) {
                            return false;
                        }
                    }
                }
                storage.lastApplied(commandIndex, commandTerm);
                return true;
            });
        }

        if (command.finish()) {
            LOG.info(
                    "Finish building the index [tableId={}, partitionId={}, indexId={}].",
                    storage.tableId(), storage.partitionId(), command.indexId()
            );
        }

        return EMPTY_APPLIED_RESULT;
    }

    private BuildIndexRowVersionChooser createBuildIndexRowVersionChooser(IndexMeta indexMeta, Set<UUID> abortedTransactionIds) {
        MetaIndexStatusChange registeredChangeInfo = indexMeta.statusChange(REGISTERED);
        MetaIndexStatusChange buildingChangeInfo = indexMeta.statusChange(BUILDING);

        return new BuildIndexRowVersionChooser(
                storage,
                registeredChangeInfo.activationTimestamp(),
                buildingChangeInfo.activationTimestamp(),
                abortedTransactionIds
        );
    }

    private BinaryRowUpgrader createBinaryRowUpgrader(IndexMeta indexMeta) {
        SchemaDescriptor schema = schemaRegistry.schema(indexMeta.tableVersion());

        return new BinaryRowUpgrader(schemaRegistry, schema);
    }

    private static BinaryRowAndRowId upgradeBinaryRow(BinaryRowUpgrader upgrader, BinaryRowAndRowId source) {
        BinaryRow sourceBinaryRow = source.binaryRow();
        assert sourceBinaryRow != null : "rowId=" + source.rowId();
        BinaryRow upgradedBinaryRow = upgrader.upgrade(sourceBinaryRow);

        return upgradedBinaryRow == sourceBinaryRow ? source : new BinaryRowAndRowId(upgradedBinaryRow, source.rowId());
    }

    private RowId toRowId(UUID rowUuid) {
        return new RowId(storageUpdateHandler.partitionId(), rowUuid);
    }
}
