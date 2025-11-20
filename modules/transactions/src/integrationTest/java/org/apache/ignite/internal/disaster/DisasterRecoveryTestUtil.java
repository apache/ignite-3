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

package org.apache.ignite.internal.disaster;

import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Statement;
import org.apache.ignite.internal.metastorage.dsl.Statement.UpdateStatement;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for disaster recovery tests.
 */
class DisasterRecoveryTestUtil {
    static void blockMessage(Cluster cluster, BiPredicate<String, NetworkMessage> predicate) {
        cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).forEach(node -> {
            BiPredicate<String, NetworkMessage> oldPredicate = node.dropMessagesPredicate();

            if (oldPredicate == null) {
                node.dropMessages(predicate);
            } else {
                node.dropMessages(oldPredicate.or(predicate));
            }
        });
    }

    static boolean stableKeySwitchMessage(
            NetworkMessage msg,
            ZonePartitionId partId,
            Assignments blockedAssignments
    ) {
        return stableKeySwitchMessage(msg, partId, blockedAssignments, null);
    }

    static boolean stableKeySwitchMessage(
            NetworkMessage msg,
            ZonePartitionId partId,
            Assignments blockedAssignments,
            @Nullable AtomicBoolean reached
    ) {
        if (msg instanceof WriteActionRequest) {
            var writeActionRequest = (WriteActionRequest) msg;
            WriteCommand command = writeActionRequest.deserializedCommand();

            if (command instanceof MultiInvokeCommand) {
                MultiInvokeCommand multiInvokeCommand = (MultiInvokeCommand) command;

                Statement andThen = multiInvokeCommand.iif().andThen();

                if (andThen instanceof UpdateStatement) {
                    UpdateStatement updateStatement = (UpdateStatement) andThen;
                    List<Operation> operations = updateStatement.update().operations();

                    ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(partId);

                    for (Operation operation : operations) {
                        ByteArray opKey = new ByteArray(toByteArray(operation.key()));

                        if (operation.type() == OperationType.PUT && opKey.equals(stablePartAssignmentsKey)) {
                            boolean equals = blockedAssignments.equals(Assignments.fromBytes(toByteArray(operation.value())));

                            if (reached != null && equals) {
                                reached.set(true);
                            }

                            return equals;
                        }
                    }
                }
            }
        }

        return false;
    }

    static void assertValueOnSpecificNodes(
            String tableName,
            Set<IgniteImpl> nodes,
            int id,
            int val,
            SchemaDescriptor schema
    ) throws Exception {
        for (IgniteImpl node : nodes) {
            assertValueOnSpecificNode(tableName, node, id, val, schema);
        }
    }

    static void assertValueOnSpecificNode(String tableName, IgniteImpl node, int id, int val, SchemaDescriptor schema) throws Exception {
        InternalTable internalTable = unwrapTableViewInternal(node.tables().table(tableName)).internalTable();

        Row keyValueRow0 = createKeyValueRow(schema, id, val);
        Row keyRow0 = createKeyRow(schema, id);

        assertTrue(waitForCondition(() -> {
            try {
                CompletableFuture<BinaryRow> getFut = internalTable.get(keyRow0, node.clock().now(), node.node());

                return compareRows(getFut.get(), keyValueRow0);
            } catch (Exception e) {
                return false;
            }
        }, 10_000), "Row comparison failed within the timeout.");
    }

    static Row createKeyValueRow(SchemaDescriptor schema, int id, int value) {
        RowAssembler rowBuilder = new RowAssembler(schema, -1);

        rowBuilder.appendInt(id);
        rowBuilder.appendInt(value);

        return Row.wrapBinaryRow(schema, rowBuilder.build());
    }

    private static boolean compareRows(BinaryRow row1, BinaryRow row2) {
        return row1.schemaVersion() == row2.schemaVersion() && row1.tupleSlice().equals(row2.tupleSlice());
    }

    private static Row createKeyRow(SchemaDescriptor schema, int id) {
        RowAssembler rowBuilder = new RowAssembler(schema.version(), schema.keyColumns(), -1);

        rowBuilder.appendInt(id);

        return Row.wrapKeyOnlyBinaryRow(schema, rowBuilder.build());
    }
}
