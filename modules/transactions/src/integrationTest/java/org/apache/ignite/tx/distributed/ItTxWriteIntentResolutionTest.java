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

package org.apache.ignite.tx.distributed;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.table.NodeUtils.transferPrimary;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runInExecutor;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.binaryRowToTuple;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.partitionReplicaListener;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.table;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.tableId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.tupleToBinaryRow;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.txId;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.zoneId;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.IgniteUtils.flatArray;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.wrapper.Wrappers.unwrap;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.ConfigOverride;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.ReadOnlyTransactionImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.message.TxStateCoordinatorRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for write intent resolution in distributed transactions.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "20")
@ConfigOverride(name = "ignite.system.properties.txnResourceTtl", value = "0")
public class ItTxWriteIntentResolutionTest extends ClusterPerClassIntegrationTest {
    private static final Tuple INITIAL_TUPLE = Tuple.create().set("key", 1L).set("val", "1");

    private static final Function<Tuple, Tuple> NEXT_TUPLE = t -> Tuple.create()
            .set("key", t.longValue("key") + 1)
            .set("val", "" + (t.longValue("key") + 1));

    /** Table messages factory. */
    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME_CP = "TEST_TABLE_CP";

    private static final String ZONE_NAME_CP = "TEST_ZONE_CP";

    private static final int REPLICAS = 3;

    private static final String VALUE_OLD = "old";
    private static final String VALUE_WRITE_INTENT = "writeIntent";
    private static final String VALUE_AFTER_READ_TS = "versionAfterReadTs";

    private final ExecutorService storageExecutor = Executors.newSingleThreadExecutor(
            IgniteThreadFactory.create(
                    "test",
                    "storage-test-pool-itwirt",
                    log,
                    ThreadOperation.STORAGE_READ,
                    ThreadOperation.STORAGE_WRITE
            )
    );

    private Tuple tuple = INITIAL_TUPLE;
    private Tuple keyTuple;

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[]{0, 1, 2};
    }

    @Override
    protected int initialNodes() {
        return 3;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        super.configureInitParameters(builder);

        builder.clusterConfiguration("ignite {"
                + "  system.properties.txnResourceTtl: \"0\""
                + "}");
    }

    @BeforeEach
    public void setup() {
        runningNodesList().forEach(n -> n.dropMessages(null));

        sql(format("create zone if not exists {} (partitions 1, replicas {}) storage profiles ['{}']",
                ZONE_NAME, REPLICAS, DEFAULT_STORAGE_PROFILE));
        sql(format("create table if not exists {} (key bigint primary key, val varchar(20)) zone {}", TABLE_NAME, ZONE_NAME));

        sql(format("create zone if not exists {} (partitions 1, replicas {}) storage profiles ['{}']",
                ZONE_NAME_CP, 1, DEFAULT_STORAGE_PROFILE));
        sql(format("create table if not exists {} (key bigint primary key, val varchar(20)) zone {}", TABLE_NAME_CP, ZONE_NAME_CP));

        tuple = NEXT_TUPLE.apply(tuple);
        keyTuple = Tuple.create().set("key", tuple.longValue("key"));
    }

    @AfterEach
    public void tearDown() {
        shutdownAndAwaitTermination(storageExecutor, 10, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @MethodSource("testArgFactory")
    public void writeIntentResolutionPrimaryReplicaPathTest(
            boolean requestByRwTxn,
            boolean writeIntentHasThisTxId,
            boolean keepWriteIntent,
            boolean commitWriteIntent,
            boolean newestCommitTsIsPresent,
            boolean addVersionAfterReadTs,
            boolean changePrimaryOnWiResolution,
            IgniteBiTuple<Class<Exception>, String> expected
    ) {
        int zoneCpId = zoneId(anyNode(), TABLE_NAME_CP);
        ZonePartitionId commitPartitionId = new ZonePartitionId(zoneCpId, 0);

        ReplicaMeta commmitPartitionReplicaMeta = waitAndGetPrimaryReplica(anyNode(), commitPartitionId);
        IgniteImpl commitPartitionPrimaryNode = findNode(node -> node.id().equals(commmitPartitionReplicaMeta.getLeaseholderId()));

        IgniteImpl coordinator = commitPartitionPrimaryNode;

        int zoneId = zoneId(coordinator, TABLE_NAME);
        int tableId = tableId(coordinator, TABLE_NAME);

        Tuple wiTuple = tuple;
        ZonePartitionId wiGroupId = new ZonePartitionId(zoneId, 0);

        blockCleanup(null, wiGroupId, tableId, false);

        ReplicaMeta wiPartitionReplicaMeta = waitAndGetPrimaryReplica(coordinator, wiGroupId);
        IgniteImpl wiPrimaryNode;

        if (wiPartitionReplicaMeta.getLeaseholderId().equals(commitPartitionPrimaryNode.id())) {
            wiPrimaryNode = findNode(node -> !node.id().equals(commitPartitionPrimaryNode.id()));
            transferPrimary(runningNodesList(), wiGroupId, node -> node.equals(wiPrimaryNode.name()));
        } else {
            wiPrimaryNode = findNode(node -> node.id().equals(wiPartitionReplicaMeta.getLeaseholderId()));
        }

        IgniteImpl wiBackupNode =
                findNode(node -> !node.id().equals(wiPrimaryNode.id()) && !node.id().equals(commitPartitionPrimaryNode.id()));

        log.info("Test: coordinator={}, commitPartitionPrimaryNode={}, wiPrimaryNode={}, wiBackupNode={}",
                coordinator.name(), commitPartitionPrimaryNode.name(), wiPrimaryNode.name(), wiBackupNode.name());

        RecordView<Tuple> viewCp = coordinator.tables().table(TABLE_NAME_CP).recordView();
        RecordView<Tuple> view = coordinator.tables().table(TABLE_NAME).recordView();

        if (newestCommitTsIsPresent) {
            view.upsert(null, wiTuple.set("val", VALUE_OLD));
        }

        Transaction rwTx = coordinator.transactions().begin();
        Transaction wiCreator = writeIntentHasThisTxId ? rwTx : coordinator.transactions().begin();
        UUID wiCreatorTxId = txId(wiCreator);

        viewCp.upsert(wiCreator, tuple);
        view.upsert(wiCreator, tuple.set("val", VALUE_WRITE_INTENT));
        // Don't finish wiCreator.

        waitForReplication(wiGroupId, tableId);

        log.info("Test: write intent creator txId={}", wiCreatorTxId);

        Collection<IgniteImpl> nodesToClearMeta = List.of(coordinator, wiPrimaryNode, wiBackupNode);

        if (!keepWriteIntent) {
            blockCleanup(wiBackupNode, wiGroupId, tableId, true);

            if (commitWriteIntent) {
                wiCreator.commit();
            } else {
                wiCreator.rollback();
            }

            waitForReplication(wiGroupId, tableId, nodesToClearMeta);
            waitForTxStateVacuum(nodesToClearMeta, wiCreatorTxId, 0, true, 10_000);
        }

        // Delete tx meta from coordinator and commit partition to force primary replica path for WI resolution.
        runningNodesList().forEach(node -> node.txManager().updateTxMeta(wiCreatorTxId, old -> null));
        coordinator.txManager().updateTxMeta(wiCreatorTxId, old -> null);
        commitPartitionPrimaryNode.txManager().updateTxMeta(wiCreatorTxId, old -> null);

        log.info("Test: tx state metas deleted.");

        if (requestByRwTxn) {
            transferPrimary(runningNodesList(), wiGroupId, node -> node.equals(wiBackupNode.name()));

            Transaction readerTx = coordinator.transactions().begin();

            wiBackupNode.dropMessages((rcp, msg) -> {
                if (msg instanceof TxStateCoordinatorRequest) {
                    throw new RuntimeException("test");
                }

                return false;
            });

            if (changePrimaryOnWiResolution) {
                wiBackupNode.dropMessages((rcp, msg) -> {
                    if (msg instanceof TxStateCommitPartitionRequest) {
                        // Change back.
                        transferPrimary(runningNodesList(), wiGroupId, node -> node.equals(wiPrimaryNode.name()));
                    }

                    return false;
                });
            }

            CompletableFuture<Tuple> tupleFut = view.getAsync(readerTx, keyTuple);

            if (expected.get1() != null) {
                assertThat(tupleFut, willThrowWithCauseOrSuppressed(expected.get1()));
            } else {
                assertThat(tupleFut, willCompleteSuccessfully());
                Tuple t = tupleFut.join();
                assertEquals(VALUE_OLD, t.stringValue("val"));
            }
        } else {
            Transaction roTx = coordinator.transactions().begin(new TransactionOptions().readOnly(true));
            HybridTimestamp readTs = unwrap(roTx, ReadOnlyTransactionImpl.class).readTimestamp();

            if (addVersionAfterReadTs) {
                view.upsert(null, wiTuple.set("val", VALUE_AFTER_READ_TS));
            }

            UUID roTxId = txId(roTx);

            ReadOnlySingleRowPkReplicaRequest req = roGetRequest(coordinator, wiGroupId, tableId, keyTuple, roTxId, readTs);
            CompletableFuture<?> reqFut = wiBackupNode.replicaService().invoke(wiBackupNode.name(), req);

            if (expected.get1() != null) {
                assertThat(reqFut, willThrow(expected.get1()));
            } else {
                assertThat(reqFut, willCompleteSuccessfully());
                Object resp = reqFut.join();

                if (expected.get2() == null) {
                    assertNull(resp);
                } else {
                    assertThat(resp, instanceOf(BinaryRow.class));
                    Tuple t = binaryRowToTuple(anyNode(), TABLE_NAME, (BinaryRow) resp, null);
                    assertEquals(expected.get2(), t.stringValue("val"));
                }
            }
        }
    }

    private static Stream<Arguments> testArgFactory() {
        boolean[] rwTxnWriteIntentOwn = { true, true };
        boolean[] rwTxnWriteIntentAnotherTxn = { true, false };
        boolean[] roTxn = { false, false };

        boolean[] keep = { true, false };
        boolean[] commit = { false, true };
        boolean[] abort = { false, false };

        boolean newestAbsent = false;
        boolean newestPresent = true;

        boolean addVersionAfter = true;
        boolean noVersionAfter = false;

        boolean noChangePrimary = false;
        boolean changePrimaryOnWiResolution = true;

        var abortedNoOld = expectedTxStateArg(null);
        var abortedWithOld = expectedTxStateArg(VALUE_OLD);
        var committed = expectedTxStateArg(VALUE_WRITE_INTENT);
        var rwException = new IgniteBiTuple<>(PrimaryReplicaMissException.class, null);

        return Stream.of(
                argumentSet(
                        "ro txn, keep WI, no old version, no version after read ts",
                        flatArray(roTxn, keep, newestAbsent, noVersionAfter, noChangePrimary, abortedNoOld)
                ),
                argumentSet(
                        "ro txn, keep WI, add old version, no version after read ts",
                        flatArray(roTxn, keep, newestPresent, noVersionAfter, noChangePrimary, abortedWithOld)
                ),
                argumentSet(
                        "ro txn, commit WI, no old version, no version after read ts",
                        flatArray(roTxn, commit, newestAbsent, noVersionAfter, noChangePrimary, committed)
                ),
                argumentSet(
                        "ro txn, commit WI, add old version, no version after read ts",
                        flatArray(roTxn, commit, newestPresent, noVersionAfter, noChangePrimary, committed)
                ),
                argumentSet(
                        "ro txn, commit WI, add old version, add version after read ts",
                        flatArray(roTxn, commit, newestPresent, addVersionAfter, noChangePrimary, committed)
                ),
                argumentSet(
                        "ro txn, abort WI, no old version, no version after read ts",
                        flatArray(roTxn, abort, newestAbsent, noVersionAfter, noChangePrimary, abortedNoOld)
                ),
                argumentSet(
                        "ro txn, abort WI, add old version, no version after read ts",
                        flatArray(roTxn, abort, newestPresent, noVersionAfter, noChangePrimary, abortedWithOld)
                ),
                argumentSet(
                        "ro txn, abort WI, add old version, add version after read ts",
                        flatArray(roTxn, abort, newestPresent, addVersionAfter, noChangePrimary, abortedWithOld)
                ),
                argumentSet(
                        "rw txn, keep WI",
                        flatArray(rwTxnWriteIntentAnotherTxn, keep, newestPresent, noVersionAfter, noChangePrimary, abortedWithOld)
                ),
                argumentSet(
                        "rw txn, keep WI, own WI",
                        flatArray(rwTxnWriteIntentOwn, keep, newestPresent, noVersionAfter, noChangePrimary, abortedWithOld)
                ),
                argumentSet(
                        "rw txn, keep WI, change primary on WI resolution",
                        flatArray(rwTxnWriteIntentAnotherTxn, keep, newestPresent, noVersionAfter, changePrimaryOnWiResolution,
                                rwException)
                )
        );
    }

    private void blockCleanup(@Nullable IgniteImpl node, ZonePartitionId groupId, int tableId, boolean blocked) {
        if (node != null) {
            partitionReplicaListener(node, groupId, tableId).storageUpdateHandler().writeIntentSwitchBlocked(blocked);
        } else {
            runningNodesStream().forEach(n -> {
                partitionReplicaListener(unwrapIgniteImpl(n), groupId, tableId).storageUpdateHandler().writeIntentSwitchBlocked(blocked);
            });
        }
    }

    private ReadOnlySingleRowPkReplicaRequest roGetRequest(
            IgniteImpl coordinator,
            ZonePartitionId groupId,
            int tableId,
            Tuple keyTuple,
            UUID txId,
            HybridTimestamp readTs
    ) {
        BinaryRowEx keyRow = tupleToBinaryRow(anyNode(), TABLE_NAME, keyTuple, null, true);

        return TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, groupId))
                .tableId(tableId)
                .schemaVersion(keyRow.schemaVersion())
                .primaryKey(keyRow.tupleSlice())
                .requestType(RO_GET)
                .readTimestamp(readTs)
                .transactionId(txId)
                .coordinatorId(coordinator.id())
                .build();
    }

    private static IgniteBiTuple<Class<Exception>, String> expectedTxStateArg(@Nullable String value) {
        return new IgniteBiTuple<>(null, value);
    }

    private void waitForReplication(ZonePartitionId groupId, int tableId) {
        waitForReplication(groupId, tableId, runningNodesStream().map(TestWrappers::unwrapIgniteImpl).collect(toList()));
    }

    private void waitForReplication(ZonePartitionId groupId, int tableId, Collection<IgniteImpl> nodes) {
        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(anyNode(), groupId);

        IgniteImpl leaseholder = findNode(node -> node.name().equals(replicaMeta.getLeaseholder()));

        HybridTimestamp primaryTime = partitionReplicaListener(leaseholder, groupId, tableId).safeTime().current()
                .tick();

        log.info("Test: waiting for replication to all nodes with primaryTime={}", primaryTime);

        List<CompletableFuture<Void>> futures = nodes.stream()
                .map(node -> partitionReplicaListener(unwrapIgniteImpl(node), groupId, tableId).safeTime().waitFor(primaryTime))
                .collect(toList());

        assertThat(allOf(futures), willCompleteSuccessfully());
    }

    /**
     * Waits for vacuum of volatile (and if needed, persistent) state of the given tx on the given nodes.
     *
     * @param nodes Nodes.
     * @param txId Transaction id.
     * @param partId Commit partition id to check the persistent tx state storage of this partition.
     * @param checkPersistent Whether to wait for vacuum of persistent tx state as well.
     * @param timeMs Time to wait.
     */
    private void waitForTxStateVacuum(Collection<IgniteImpl> nodes, UUID txId, int partId, boolean checkPersistent, long timeMs) {
        try {
            await()
                    .atMost(timeMs, TimeUnit.MILLISECONDS)
                    .until(() -> txStateIsAbsent(nodes, txId, TABLE_NAME, partId, checkPersistent, false));
        } catch (AssertionError e) {
            logCurrentTxState(nodes, txId, TABLE_NAME, partId);
            throw e;
        }
    }

    /**
     * Checks whether the tx state is absent on all of the given nodes.
     *
     * @param nodes Nodes.
     * @param txId Transaction id.
     * @param tableName Table name of the table that commit partition belongs to.
     * @param partId Commit partition id.
     * @param checkPersistent Whether the persistent state should be checked.
     * @param checkCpPrimaryOnly If {@code} true, the persistent state should be checked only on the commit partition primary,
     *     otherwise it would be checked on every given node.
     * @return {@code true} if tx state is absent, {@code false} otherwise. Call {@link #logCurrentTxState(Collection, UUID, String, int)}
     *     for details.
     */
    private boolean txStateIsAbsent(
            Collection<IgniteImpl> nodes,
            UUID txId,
            String tableName,
            int partId,
            boolean checkPersistent,
            boolean checkCpPrimaryOnly
    ) {
        boolean result = true;

        UUID cpPrimaryId = null;

        if (checkCpPrimaryOnly) {
            IgniteImpl node = anyNode();

            ZonePartitionId partitionGroupId = new ZonePartitionId(zoneId(node, tableName), partId);

            CompletableFuture<ReplicaMeta> replicaFut = node.placementDriver().getPrimaryReplica(partitionGroupId, node.clock().now());
            assertThat(replicaFut, willCompleteSuccessfully());

            ReplicaMeta replicaMeta = replicaFut.join();
            // The test doesn't make sense if there is no primary right now.
            assertNotNull(replicaMeta);

            cpPrimaryId = replicaMeta.getLeaseholderId();
        }

        for (Iterator<IgniteImpl> iterator = nodes.iterator(); iterator.hasNext();) {
            IgniteImpl node = iterator.next();

            result = result
                    && volatileTxState(node, txId) == null
                    && (!checkPersistent || !node.id().equals(cpPrimaryId) || persistentTxState(node, txId, tableName, partId) == null);
        }

        return result;
    }

    private void logCurrentTxState(Collection<IgniteImpl> nodes, UUID txId, String table, int partId) {
        nodes.stream()
                .map(TestWrappers::unwrapIgniteImpl)
                .forEach(node -> {
                    log.info("Test: volatile   state [tx={}, node={}, state={}].", txId, node.name(), volatileTxState(node, txId));
                    log.info(
                            "Test: persistent state [tx={}, node={}, state={}].",
                            txId, node.name(), persistentTxState(node, txId, table, partId)
                    );
                });
    }

    @Nullable
    private static TransactionMeta volatileTxState(IgniteImpl node, UUID txId) {
        TxManagerImpl txManager = (TxManagerImpl) node.txManager();

        TxStateMeta txInMemoryState = txManager.stateMeta(txId);

        if (txInMemoryState == null) {
            return null;
        }

        if (TxState.isFinalState(txInMemoryState.txState())) {
            long current = FastTimestamps.coarseCurrentTimeMillis();

            Long initialTs = txInMemoryState.initialVacuumObservationTimestamp();

            assertNotNull(initialTs);

            assertTrue(current >= initialTs);
        }

        return txInMemoryState;
    }

    @Nullable
    private TransactionMeta persistentTxState(IgniteImpl node, UUID txId, String tableName, int partId) {
        return runInExecutor(storageExecutor, () -> {
            InternalTable internalTable = table(node, tableName).internalTable();

            TxStatePartitionStorage txStatePartitionStorage = node
                    .partitionReplicaLifecycleManager()
                    .txStatePartitionStorage(internalTable.zoneId(), partId);

            assertNotNull(txStatePartitionStorage);

            return txStatePartitionStorage.get(txId);
        });
    }
}
