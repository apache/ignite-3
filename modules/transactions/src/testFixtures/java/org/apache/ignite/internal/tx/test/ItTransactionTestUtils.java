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

package org.apache.ignite.internal.tx.test;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.table.RecordBinaryViewImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Test utils for transaction integration tests.
 */
public class ItTransactionTestUtils {
    /**
     * Get the names of the nodes that are assignments of the given partition.
     *
     * @param node Any node in the cluster.
     * @param grpId Group id.
     * @return Node names.
     */
    public static Set<String> partitionAssignment(IgniteImpl node, PartitionGroupId grpId) {
        MetaStorageManager metaStorageManager = node.metaStorageManager();

        ByteArray stableAssignmentKey = stablePartAssignmentsKey((ZonePartitionId) grpId);

        CompletableFuture<Entry> assignmentEntryFut = metaStorageManager.get(stableAssignmentKey);

        assertThat(assignmentEntryFut, willCompleteSuccessfully());

        Entry e = assignmentEntryFut.join();

        assertNotNull(e);
        assertFalse(e.empty());
        assertFalse(e.tombstone());

        Set<Assignment> a = requireNonNull(Assignments.fromBytes(e.value())).nodes();

        return a.stream().filter(Assignment::isPeer).map(Assignment::consistentId).collect(toSet());
    }

    /**
     * Calculate the partition id on which the given tuple would be placed.
     *
     * @param node Any node in the cluster.
     * @param tableName Table name.
     * @param tuple Data tuple.
     * @param tx Transaction, if present.
     * @return Partition id.
     */
    public static int partitionIdForTuple(IgniteImpl node, String tableName, Tuple tuple, @Nullable Transaction tx) {
        TableImpl table = table(node, tableName);
        RecordBinaryViewImpl view = unwrapRecordBinaryViewImpl(table.recordView());

        CompletableFuture<BinaryRowEx> rowFut = view.tupleToBinaryRow(tx, tuple);
        assertThat(rowFut, willCompleteSuccessfully());
        BinaryRowEx row = rowFut.join();

        return table.internalTable().partitionId(row);
    }

    /**
     * Generates some tuple that would be placed in the partition that is hosted on the given node in the cluster.
     *
     * @param node Node that should host the result tuple.
     * @param tableName Table name.
     * @param tx Transaction, if present.
     * @param initialTuple Initial tuple, for calculation.
     * @param nextTuple This function will be used to generate new tuples in order to find suitable one.
     * @param primary Whether the given node should be the primary node.
     * @return Tuple that would be placed on the given node.
     */
    public static Tuple findTupleToBeHostedOnNode(
            IgniteImpl node,
            String tableName,
            @Nullable Transaction tx,
            Tuple initialTuple,
            Function<Tuple, Tuple> nextTuple,
            boolean primary
    ) {
        Tuple t = initialTuple;

        Set<Integer> partitionIds = new HashSet<>();
        Set<String> nodes = new HashSet<>();

        final int maxAttempts = 1000;
        int attempts = maxAttempts;

        while (attempts >= 0) {
            int partId = partitionIdForTuple(node, tableName, t, tx);
            partitionIds.add(partId);

            ZonePartitionId grpId = new ZonePartitionId(zoneId(node, tableName), partId);

            if (primary) {
                ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, grpId);

                if (node.id().equals(replicaMeta.getLeaseholderId())) {
                    return t;
                }

                nodes.add(replicaMeta.getLeaseholder());
            } else {
                Set<String> assignments = partitionAssignment(node, grpId);

                if (assignments.contains(node.name())) {
                    return t;
                }

                nodes.addAll(assignments);
            }

            t = nextTuple.apply(t);

            attempts--;
        }

        throw new AssertionError("Failed to find a suitable tuple, tried " + maxAttempts + " times with [partitionIds="
                + partitionIds + ", nodes=" + nodes + "].");
    }

    /**
     * Returns table instance.
     *
     * @param node Ignite node.
     * @param tableName Table name.
     * @return Table instance.
     */
    public static TableImpl table(Ignite node, String tableName) {
        return unwrapTableImpl(node.tables().table(tableName));
    }

    /**
     * Returns the table id.
     *
     * @param node Any node in the cluster.
     * @param tableName Table name.
     * @return Table id.
     */
    public static int tableId(Ignite node, String tableName) {
        return table(node, tableName).tableId();
    }

    /**
     * Returns the zone id.
     *
     * @param node Any node in the cluster.
     * @param tableName Table name.
     * @return Zone id.
     */
    public static int zoneId(Ignite node, String tableName) {
        return table(node, tableName).zoneId();
    }

    /**
     * Transaction id.
     *
     * @param tx Transaction.
     * @return Transaction id.
     */
    public static UUID txId(Transaction tx) {
        return ((ReadWriteTransactionImpl) unwrapIgniteTransaction(tx)).id();
    }

    /**
     * Waits for the primary replica appearance for the given replication group and returns it.
     *
     * @param node Any node in the cluster.
     * @param replicationGrpId Replication group.
     * @return Primary replica meta.
     */
    public static ReplicaMeta waitAndGetPrimaryReplica(IgniteImpl node, ZonePartitionId replicationGrpId) {
        CompletableFuture<ReplicaMeta> primaryReplicaFut = node.placementDriver().awaitPrimaryReplica(
                replicationGrpId,
                node.clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        return primaryReplicaFut.join();
    }

    /**
     * Executes a closure in a transaction.
     *
     * @param transactions Transactions facade.
     * @param c The closure.
     */
    public static void withTxVoid(IgniteTransactions transactions, Consumer<Transaction> c) {
        Transaction tx = transactions.begin();
        c.accept(tx);
        tx.commit();
    }

    /**
     * Executes a closure in a transaction.
     *
     * @param transactions Transactions facade.
     * @param c The closure.
     */
    public static <T> T withTx(IgniteTransactions transactions, Function<Transaction, T> c) {
        Transaction tx = transactions.begin();
        T t = c.apply(tx);
        tx.commit();
        return t;
    }

    /**
     * Unwraps {@link RecordBinaryViewImpl} from a {@link RecordView}.
     *
     * @param view View to unwrap.
     */
    private static RecordBinaryViewImpl unwrapRecordBinaryViewImpl(RecordView view) {
        return Wrappers.unwrap(view, RecordBinaryViewImpl.class);
    }

    /**
     * Unwraps {@link TableImpl} from a {@link Table}.
     *
     * @param table Table to unwrap.
     */
    private static TableImpl unwrapTableImpl(Table table) {
        return Wrappers.unwrap(table, TableImpl.class);
    }

    /**
     * Unwraps {@link Transaction} from an {@link Transaction}.
     *
     * @param tx Object to unwrap.
     */
    private static Transaction unwrapIgniteTransaction(Transaction tx) {
        return Wrappers.unwrap(tx, Transaction.class);
    }
}
