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

package org.apache.ignite.internal.table.distributed.command;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.message.EnlistedPartitionGroupMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for partition RAFT commands serialization.
 */
public class PartitionRaftCommandsSerializationTest extends IgniteAbstractTest {
    private static final int TABLE_ID = 1;

    /** Hybrid clock. */
    private final HybridClock clock = new HybridClockImpl();

    /** Key-value marshaller for tests. */
    private static KvMarshaller<TestKey, TestValue> kvMarshaller;

    /** Message factory to create messages - RAFT commands. */
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    @BeforeAll
    static void beforeAll() {
        var marshallerFactory = new ReflectionMarshallerFactory();

        var schemaDescriptor = new SchemaDescriptor(1, new Column[]{
                new Column("intKey".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strKey".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        }, new Column[]{
                new Column("intVal".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strVal".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        });

        kvMarshaller = marshallerFactory.create(schemaDescriptor, TestKey.class, TestValue.class);
    }

    @Test
    public void testUpdateCommand() throws Exception {
        TablePartitionIdMessage tablePartitionIdMessage = REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                .tableId(TABLE_ID)
                .partitionId(1)
                .build();

        UpdateCommand cmd = PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                .tableId(TABLE_ID)
                .commitPartitionId(tablePartitionIdMessage)
                .rowUuid(UUID.randomUUID())
                .messageRowToUpdate(PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                                .binaryRowMessage(binaryRowMessage(1))
                                .build())
                .txId(TestTransactionIds.newTransactionId())
                .txCoordinatorId(UUID.randomUUID())
                .initiatorTime(clock.now())
                .build();

        UpdateCommand readCmd = copyCommand(cmd);

        assertEquals(cmd, readCmd);
    }

    @Test
    public void testRemoveCommand() {
        TablePartitionIdMessage tablePartitionIdMessage = REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                .tableId(1)
                .partitionId(1)
                .build();

        UpdateCommand cmd = PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                .tableId(TABLE_ID)
                .commitPartitionId(tablePartitionIdMessage)
                .rowUuid(UUID.randomUUID())
                .txId(TestTransactionIds.newTransactionId())
                .txCoordinatorId(UUID.randomUUID())
                .initiatorTime(clock.now())
                .build();

        UpdateCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.rowUuid(), readCmd.rowUuid());
        assertNull(readCmd.rowToUpdate());
    }

    @Test
    public void testUpdateAllCommand() throws Exception {
        Map<UUID, TimedBinaryRowMessage> rowsToUpdate = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            rowsToUpdate.put(
                    TestTransactionIds.newTransactionId(),
                    PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                            .binaryRowMessage(binaryRowMessage(i))
                            .timestamp(i % 2 == 0 ? clock.now() : null)
                            .build()
            );
        }

        TablePartitionIdMessage tablePartitionIdMessage = REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                .tableId(1)
                .partitionId(1)
                .build();

        var cmd = PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                .tableId(TABLE_ID)
                .commitPartitionId(tablePartitionIdMessage)
                .messageRowsToUpdate(rowsToUpdate)
                .txId(UUID.randomUUID())
                .txCoordinatorId(UUID.randomUUID())
                .initiatorTime(clock.now())
                .build();

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (Map.Entry<UUID, TimedBinaryRowMessage> entry : cmd.messageRowsToUpdate().entrySet()) {
            assertTrue(readCmd.rowsToUpdate().containsKey(entry.getKey()));

            var readVal = readCmd.rowsToUpdate().get(entry.getKey()).binaryRow();
            var val = entry.getValue().binaryRow();

            assertEquals(val, readVal);

            HybridTimestamp readTs = readCmd.rowsToUpdate().get(entry.getKey()).commitTimestamp();
            HybridTimestamp ts = entry.getValue().timestamp();

            assertEquals(ts, readTs);
        }
    }

    @Test
    public void testRemoveAllCommand() {
        Map<UUID, TimedBinaryRowMessage> rowsToRemove = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            rowsToRemove.put(TestTransactionIds.newTransactionId(), PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                    .build());
        }

        TablePartitionIdMessage tablePartitionIdMessage = REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                .tableId(1)
                .partitionId(1)
                .build();

        var cmd = PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                .tableId(TABLE_ID)
                .commitPartitionId(tablePartitionIdMessage)
                .messageRowsToUpdate(rowsToRemove)
                .txId(UUID.randomUUID())
                .txCoordinatorId(UUID.randomUUID())
                .initiatorTime(clock.now())
                .build();

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (UUID uuid : cmd.messageRowsToUpdate().keySet()) {
            assertTrue(readCmd.rowsToUpdate().containsKey(uuid));
            assertNull(readCmd.rowsToUpdate().get(uuid).binaryRow());
        }
    }

    @Test
    public void testTxCleanupCommand() {
        WriteIntentSwitchCommand cmd = PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(UUID.randomUUID())
                .commit(true)
                .tableIds(Set.of(1))
                .initiatorTime(clock.now())
                .commitTimestamp(clock.now())
                .build();

        WriteIntentSwitchCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.commit(), readCmd.commit());
        assertEquals(cmd.commitTimestamp(), readCmd.commitTimestamp());
    }

    @Test
    public void testFinishTxCommand() {
        ArrayList<ZonePartitionIdMessage> grps = new ArrayList<>(10);

        for (int i = 0; i < 10; i++) {
            grps.add(REPLICA_MESSAGES_FACTORY.zonePartitionIdMessage()
                    .zoneId(1)
                    .partitionId(i)
                    .build());
        }

        FinishTxCommand cmd = PARTITION_REPLICATION_MESSAGES_FACTORY.finishTxCommandV2()
                .txId(UUID.randomUUID())
                .commit(true)
                .commitTimestamp(clock.now())
                .initiatorTime(clock.now())
                .partitions(grps.stream().map(grp -> partitionGroupMessage(grp, Set.of(1))).collect(toList()))
                .build();

        FinishTxCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.commit(), readCmd.commit());
        assertEquals(cmd.commitTimestamp(), readCmd.commitTimestamp());
    }

    private static EnlistedPartitionGroupMessage partitionGroupMessage(ZonePartitionIdMessage groupIdMessage, Set<Integer> tableIds) {
        return TX_MESSAGES_FACTORY.enlistedPartitionGroupMessage()
                .groupId(groupIdMessage)
                .tableIds(tableIds)
                .build();
    }

    private <T extends Command> T copyCommand(T cmd) {
        assertEquals(PartitionReplicationMessageGroup.GROUP_TYPE, cmd.groupType());

        if (cmd instanceof FinishTxCommandV2) {
            FinishTxCommandV2 finishTxCommand = (FinishTxCommandV2) cmd;

            return (T) PARTITION_REPLICATION_MESSAGES_FACTORY.finishTxCommandV2()
                    .txId(finishTxCommand.txId())
                    .commit(finishTxCommand.commit())
                    .partitions(finishTxCommand.partitions())
                    .commitTimestamp(finishTxCommand.commitTimestamp())
                    .initiatorTime(finishTxCommand.initiatorTime())
                    .build();
        } else if (cmd instanceof WriteIntentSwitchCommand) {
            WriteIntentSwitchCommand writeIntentSwitchCommand = (WriteIntentSwitchCommand) cmd;

            return (T) PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                    .txId(writeIntentSwitchCommand.txId())
                    .commit(writeIntentSwitchCommand.commit())
                    .commitTimestamp(writeIntentSwitchCommand.commitTimestamp())
                    .tableIds(Set.of(1))
                    .initiatorTime(writeIntentSwitchCommand.initiatorTime())
                    .build();
        } else if (cmd instanceof UpdateCommandV2) {
            UpdateCommandV2 updateCommand = (UpdateCommandV2) cmd;

            return (T) PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                    .txId(updateCommand.txId())
                    .rowUuid(updateCommand.rowUuid())
                    .tableId(updateCommand.tableId())
                    .commitPartitionId(updateCommand.commitPartitionId())
                    .messageRowToUpdate(updateCommand.messageRowToUpdate())
                    .txCoordinatorId(updateCommand.txCoordinatorId())
                    .initiatorTime(updateCommand.initiatorTime())
                    .build();
        } else if (cmd instanceof UpdateAllCommandV2) {
            UpdateAllCommandV2 updateCommand = (UpdateAllCommandV2) cmd;

            return (T) PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                    .txId(updateCommand.txId())
                    .messageRowsToUpdate(updateCommand.messageRowsToUpdate())
                    .tableId(updateCommand.tableId())
                    .commitPartitionId(updateCommand.commitPartitionId())
                    .txCoordinatorId(updateCommand.txCoordinatorId())
                    .initiatorTime(updateCommand.initiatorTime())
                    .build();
        } else {
            fail(cmd.toString());

            return null;
        }
    }

    private BinaryRowMessage binaryRowMessage(int id) {
        Row row = kvMarshaller.marshal(
                new TestKey(id, String.valueOf(id)),
                new TestValue(id, String.valueOf(id))
        );

        return PARTITION_REPLICATION_MESSAGES_FACTORY.binaryRowMessage()
                .binaryTuple(row.tupleSlice())
                .schemaVersion(row.schemaVersion())
                .build();
    }

    /**
     * Test pojo key.
     */
    private static class TestKey {
        public int intKey;

        public String strKey;

        public TestKey() {
        }

        public TestKey(int intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestKey testKey = (TestKey) o;
            return intKey == testKey.intKey && Objects.equals(strKey, testKey.strKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intKey, strKey);
        }
    }

    /**
     * Test pojo value.
     */
    protected static class TestValue implements Comparable<TestValue> {
        @IgniteToStringInclude
        public Integer intVal;

        @IgniteToStringInclude
        public String strVal;

        public TestValue() {
        }

        public TestValue(Integer intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        @Override
        public int compareTo(TestValue o) {
            int cmp = Integer.compare(intVal, o.intVal);

            return cmp != 0 ? cmp : strVal.compareTo(o.strVal);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestValue testValue = (TestValue) o;
            return Objects.equals(intVal, testValue.intVal) && Objects.equals(strVal, testValue.strVal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intVal, strVal);
        }

        @Override
        public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
