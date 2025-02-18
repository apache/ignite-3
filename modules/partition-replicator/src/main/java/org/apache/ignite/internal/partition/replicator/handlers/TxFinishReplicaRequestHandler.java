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

package org.apache.ignite.internal.partition.replicator.handlers;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.ReliableCatalogVersions;
import org.apache.ignite.internal.partition.replicator.ReplicaTxFinishMarker;
import org.apache.ignite.internal.partition.replicator.ReplicationRaftCommandApplicator;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommandBuilder;
import org.apache.ignite.internal.partition.replicator.raft.UnexpectedTransactionStateException;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schemacompat.CompatValidationResult;
import org.apache.ignite.internal.partition.replicator.schemacompat.SchemaCompatibilityValidator;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.IncompatibleSchemaAbortException;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeInternalException;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Handles {@link TxFinishReplicaRequest}.
 */
public class TxFinishReplicaRequestHandler {
    private static final IgniteLogger LOG = Loggers.forClass(TxFinishReplicaRequestHandler.class);

    /** Factory to create RAFT command messages. */
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    /** Factory for creating replica command messages. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final TxStatePartitionStorage txStatePartitionStorage;
    private final ClockService clockService;
    private final TxManager txManager;
    private final ReplicationGroupId replicationGroupId;

    private final SchemaCompatibilityValidator schemaCompatValidator;
    private final ReliableCatalogVersions reliableCatalogVersions;
    private final ReplicationRaftCommandApplicator raftCommandApplicator;
    private final ReplicaTxFinishMarker replicaTxFinishMarker;


    /** Constructor. */
    public TxFinishReplicaRequestHandler(
            TxStatePartitionStorage txStatePartitionStorage,
            ClockService clockService,
            TxManager txManager,
            ValidationSchemasSource validationSchemasSource,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            RaftCommandRunner raftCommandRunner,
            ReplicationGroupId replicationGroupId
    ) {
        this.txStatePartitionStorage = txStatePartitionStorage;
        this.clockService = clockService;
        this.txManager = txManager;
        this.replicationGroupId = replicationGroupId;

        schemaCompatValidator = new SchemaCompatibilityValidator(validationSchemasSource, catalogService, schemaSyncService);
        reliableCatalogVersions = new ReliableCatalogVersions(schemaSyncService, catalogService);
        raftCommandApplicator = new ReplicationRaftCommandApplicator(raftCommandRunner, replicationGroupId);
        replicaTxFinishMarker = new ReplicaTxFinishMarker(txManager);
    }

    /**
     * Processes transaction finish request.
     * <ol>
     *     <li>Get commit timestamp from finish replica request.</li>
     *     <li>If attempting a commit, validate commit (and, if not valid, switch to abort)</li>
     *     <li>Run {@code FinishTxCommand} raft command, that will apply txn state to corresponding txStateStorage.</li>
     *     <li>Send cleanup requests to all enlisted primary replicas.</li>
     * </ol>
     *
     * @param request Transaction finish request.
     * @return future result of the operation.
     */
    public CompletableFuture<TransactionResult> handle(TxFinishReplicaRequest request) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-19170 Use ZonePartitionIdMessage and remove cast
        Map<ReplicationGroupId, String> enlistedGroups = asReplicationGroupIdToStringMap(request.groups());

        UUID txId = request.txId();

        if (request.commit()) {
            HybridTimestamp commitTimestamp = request.commitTimestamp();

            return schemaCompatValidator.validateCommit(txId, request.tableIds(), commitTimestamp)
                    .thenCompose(validationResult ->
                            finishAndCleanup(
                                    enlistedGroups,
                                    validationResult.isSuccessful(),
                                    validationResult.isSuccessful() ? commitTimestamp : null,
                                    txId
                            ).thenApply(txResult -> {
                                throwIfSchemaValidationOnCommitFailed(validationResult, txResult);
                                return txResult;
                            }));
        } else {
            // Aborting.
            return finishAndCleanup(enlistedGroups, false, null, txId);
        }
    }

    private static Map<ReplicationGroupId, String> asReplicationGroupIdToStringMap(Map<ReplicationGroupIdMessage, String> messages) {
        var result = new HashMap<ReplicationGroupId, String>(IgniteUtils.capacity(messages.size()));

        for (Entry<ReplicationGroupIdMessage, String> e : messages.entrySet()) {
            result.put(e.getKey().asReplicationGroupId(), e.getValue());
        }

        return result;
    }

    private CompletableFuture<TransactionResult> finishAndCleanup(
            Map<ReplicationGroupId, String> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        // Read TX state from the storage, we will need this state to check if the locks are released.
        // Since this state is written only on the transaction finish (see PartitionListener.handleFinishTxCommand),
        // the value of txMeta can be either null or COMMITTED/ABORTED. No other values are expected.
        TxMeta txMeta = txStatePartitionStorage.get(txId);

        // Check whether a transaction has already been finished.
        boolean transactionAlreadyFinished = txMeta != null && isFinalState(txMeta.txState());

        if (transactionAlreadyFinished) {
            // - The Coordinator calls use same tx state over retries, both abort and commit are possible.
            // - Server side recovery may only change tx state to aborted.
            // - The Coordinator itself should prevent user calls with different proposed state to the one,
            //   that was already triggered (e.g. the client side -> txCoordinator.commitAsync(); txCoordinator.rollbackAsync()).
            // - A coordinator might send a commit, then die, but the commit message might still arrive at the commit partition primary.
            //   If it arrived with a delay, another node might come across a write intent/lock from that tx
            //   and realize that the coordinator is no longer available and start tx recovery.
            //   The original commit message might arrive later than the recovery one,
            //   hence a 'commit over rollback' case.
            // The possible states that a 'commit' is allowed to see:
            // - null (if it's the first change state attempt)
            // - committed (if it was already updated in the previous attempt)
            // - aborted (if it was aborted by the initiate recovery logic,
            //   though this is a very unlikely case because initiate recovery will only roll back the tx if coordinator is dead).
            //
            // Within 'roll back' it's allowed to see:
            // - null (if it's the first change state attempt)
            // - aborted  (if it was already updated in the previous attempt or the result of a concurrent recovery)
            // - commit (if initiate recovery has started, but a delayed message from the coordinator finally arrived and executed earlier).

            // Let the client know a transaction has finished with a different outcome.
            if (commit != (txMeta.txState() == COMMITTED)) {
                LOG.error("Failed to finish a transaction that is already finished [txId={}, expectedState={}, actualState={}].",
                        txId,
                        commit ? COMMITTED : ABORTED,
                        txMeta.txState()
                );

                throw new MismatchingTransactionOutcomeInternalException(
                        "Failed to change the outcome of a finished transaction [txId=" + txId + ", txState=" + txMeta.txState() + "].",
                        new TransactionResult(txMeta.txState(), txMeta.commitTimestamp())
                );
            }

            return completedFuture(new TransactionResult(txMeta.txState(), txMeta.commitTimestamp()));
        }

        return finishTransaction(enlistedPartitions.keySet(), txId, commit, commitTimestamp)
                .thenCompose(txResult ->
                    txManager.cleanup(replicationGroupId, enlistedPartitions, commit, commitTimestamp, txId)
                            .thenApply(v -> txResult)
                );
    }

    private static void throwIfSchemaValidationOnCommitFailed(CompatValidationResult validationResult, TransactionResult txResult) {
        if (!validationResult.isSuccessful()) {
            if (validationResult.isTableDropped()) {
                throw new IncompatibleSchemaAbortException(
                        format("Commit failed because a table was already dropped [table={}]", validationResult.failedTableName()),
                        txResult
                );
            } else {
                throw new IncompatibleSchemaAbortException(
                        format(
                                "Commit failed because schema is not forward-compatible "
                                        + "[fromSchemaVersion={}, toSchemaVersion={}, table={}, details={}]",
                                validationResult.fromSchemaVersion(),
                                validationResult.toSchemaVersion(),
                                validationResult.failedTableName(),
                                validationResult.details()
                        ),
                        txResult
                );
            }
        }
    }

    /**
     * Finishes a transaction. This operation is idempotent.
     *
     * @param partitionIds Collection of enlisted partition groups.
     * @param txId Transaction id.
     * @param commit True is the transaction is committed, false otherwise.
     * @param commitTimestamp Commit timestamp, if applicable.
     * @return Future to wait of the finish.
     */
    private CompletableFuture<TransactionResult> finishTransaction(
            Collection<ReplicationGroupId> partitionIds,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        assert !(commit && commitTimestamp == null) : "Cannot commit without the timestamp.";

        HybridTimestamp tsForCatalogVersion = commit ? commitTimestamp : clockService.now();

        return reliableCatalogVersionFor(tsForCatalogVersion)
                .thenCompose(catalogVersion -> applyFinishCommand(
                        txId,
                        commit,
                        commitTimestamp,
                        catalogVersion,
                        toPartitionIdMessage(partitionIds)
                ))
                .handle((txOutcome, ex) -> {
                    if (ex != null) {
                        // RAFT 'finish' command failed because the state has already been written by someone else.
                        // In that case we throw a corresponding exception.
                        if (ex instanceof UnexpectedTransactionStateException) {
                            UnexpectedTransactionStateException utse = (UnexpectedTransactionStateException) ex;
                            TransactionResult result = utse.transactionResult();

                            replicaTxFinishMarker.markFinished(txId, result.transactionState(), result.commitTimestamp());

                            throw new MismatchingTransactionOutcomeInternalException(utse.getMessage(), utse.transactionResult());
                        }
                        // Otherwise we convert from the internal exception to the client one.
                        throw new TransactionException(commit ? TX_COMMIT_ERR : TX_ROLLBACK_ERR, ex);
                    }

                    TransactionResult result = (TransactionResult) txOutcome;

                    replicaTxFinishMarker.markFinished(txId, result.transactionState(), result.commitTimestamp());

                    return result;
                });
    }

    private CompletableFuture<Integer> reliableCatalogVersionFor(HybridTimestamp ts) {
        return reliableCatalogVersions.reliableCatalogVersionFor(ts);
    }

    private CompletableFuture<Object> applyFinishCommand(
            UUID transactionId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            int catalogVersion,
            List<ReplicationGroupIdMessage> partitionIds
    ) {
        HybridTimestamp now = clockService.now();
        FinishTxCommandBuilder finishTxCmdBldr = PARTITION_REPLICATION_MESSAGES_FACTORY.finishTxCommand()
                .txId(transactionId)
                .commit(commit)
                .initiatorTime(now)
                .requiredCatalogVersion(catalogVersion)
                .partitionIds(partitionIds);

        if (commit) {
            finishTxCmdBldr.commitTimestamp(commitTimestamp);
        }

        return raftCommandApplicator.applyCommandWithExceptionHandling(finishTxCmdBldr.build());
    }

    private static List<ReplicationGroupIdMessage> toPartitionIdMessage(Collection<ReplicationGroupId> partitionIds) {
        List<ReplicationGroupIdMessage> list = new ArrayList<>(partitionIds.size());

        for (ReplicationGroupId partitionId : partitionIds) {
            list.add(replicationGroupId(partitionId));
        }

        return list;
    }

    /**
     * Method to convert from {@link ReplicationGroupId} object to command-based {@link ReplicationGroupIdMessage} object.
     *
     * @param replicationGroupId {@link ReplicationGroupId} object to convert to {@link ReplicationGroupIdMessage}.
     * @return {@link ReplicationGroupIdMessage} object converted from argument.
     */
    private static ReplicationGroupIdMessage replicationGroupId(ReplicationGroupId replicationGroupId) {
        return toReplicationGroupIdMessage(REPLICA_MESSAGES_FACTORY, replicationGroupId);
    }
}
