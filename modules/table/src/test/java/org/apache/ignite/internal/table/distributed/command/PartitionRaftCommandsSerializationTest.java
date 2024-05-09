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

import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryRowMessage;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for partition RAFT commands serialization.
 */
public class PartitionRaftCommandsSerializationTest extends IgniteAbstractTest {
    /** Hybrid clock. */
    private final HybridClockImpl clock = new HybridClockImpl();

    /** Key-value marshaller for tests. */
    private static KvMarshaller<TestKey, TestValue> kvMarshaller;

    /** Message factory to create messages - RAFT commands. */
    private final TableMessagesFactory msgFactory = new TableMessagesFactory();

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
        UpdateCommand cmd = msgFactory.updateCommand()
                .tablePartitionId(msgFactory.tablePartitionIdMessage()
                        .tableId(1)
                        .partitionId(1)
                        .build()
                )
                .rowUuid(UUID.randomUUID())
                .messageRowToUpdate(msgFactory.timedBinaryRowMessage()
                                .binaryRowMessage(binaryRowMessage(1))
                                .build())
                .txId(TestTransactionIds.newTransactionId())
                .txCoordinatorId(UUID.randomUUID().toString())
                .build();

        UpdateCommand readCmd = copyCommand(cmd);

        assertEquals(cmd, readCmd);
    }

    @Test
    public void testRemoveCommand() throws Exception {
        UpdateCommand cmd = msgFactory.updateCommand()
                .tablePartitionId(msgFactory.tablePartitionIdMessage()
                        .tableId(1)
                        .partitionId(1)
                        .build()
                )
                .rowUuid(UUID.randomUUID())
                .txId(TestTransactionIds.newTransactionId())
                .txCoordinatorId(UUID.randomUUID().toString())
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
                    msgFactory.timedBinaryRowMessage()
                            .binaryRowMessage(binaryRowMessage(i))
                            .timestamp(i % 2 == 0 ? clock.nowLong() : NULL_HYBRID_TIMESTAMP)
                            .build()
            );
        }

        var cmd = msgFactory.updateAllCommand()
                .tablePartitionId(msgFactory.tablePartitionIdMessage()
                        .tableId(1)
                        .partitionId(1)
                        .build()
                )
                .messageRowsToUpdate(rowsToUpdate)
                .txId(UUID.randomUUID())
                .txCoordinatorId(UUID.randomUUID().toString())
                .build();

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (Map.Entry<UUID, TimedBinaryRowMessage> entry : cmd.messageRowsToUpdate().entrySet()) {
            assertTrue(readCmd.rowsToUpdate().containsKey(entry.getKey()));

            var readVal = readCmd.rowsToUpdate().get(entry.getKey()).binaryRow();
            var val = entry.getValue().binaryRow();

            assertEquals(val, readVal);

            var readTs = readCmd.rowsToUpdate().get(entry.getKey()).commitTimestamp();
            var ts = nullableHybridTimestamp(entry.getValue().timestamp());

            assertEquals(ts, readTs);
        }
    }

    @Test
    public void testRemoveAllCommand() throws Exception {
        Map<UUID, TimedBinaryRowMessage> rowsToRemove = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            rowsToRemove.put(TestTransactionIds.newTransactionId(), msgFactory.timedBinaryRowMessage()
                    .build());
        }

        var cmd = msgFactory.updateAllCommand()
                .tablePartitionId(msgFactory.tablePartitionIdMessage()
                        .tableId(1)
                        .partitionId(1)
                        .build()
                )
                .messageRowsToUpdate(rowsToRemove)
                .txId(UUID.randomUUID())
                .txCoordinatorId(UUID.randomUUID().toString())
                .build();

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (UUID uuid : cmd.messageRowsToUpdate().keySet()) {
            assertTrue(readCmd.rowsToUpdate().containsKey(uuid));
            assertNull(readCmd.rowsToUpdate().get(uuid).binaryRow());
        }
    }

    @Test
    public void testTxCleanupCommand() throws Exception {
        HybridClock clock = new HybridClockImpl();

        WriteIntentSwitchCommand cmd = msgFactory.writeIntentSwitchCommand()
                .txId(UUID.randomUUID())
                .commit(true)
                .commitTimestampLong(clock.nowLong())
                .build();

        WriteIntentSwitchCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.commit(), readCmd.commit());
        assertEquals(cmd.commitTimestamp(), readCmd.commitTimestamp());
    }

    @Test
    public void testFinishTxCommand() throws Exception {
        HybridClock clock = new HybridClockImpl();
        ArrayList<TablePartitionIdMessage> grps = new ArrayList<>(10);

        for (int i = 0; i < 10; i++) {
            grps.add(msgFactory.tablePartitionIdMessage()
                    .tableId(1)
                    .partitionId(i)
                    .build());
        }

        FinishTxCommand cmd = msgFactory.finishTxCommand()
                .txId(UUID.randomUUID())
                .commit(true)
                .commitTimestampLong(clock.nowLong())
                .partitionIds(grps)
                .build();

        FinishTxCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.commit(), readCmd.commit());
        assertEquals(cmd.commitTimestamp(), readCmd.commitTimestamp());
    }

    private <T extends Command> T copyCommand(T cmd) {
        assertEquals(TableMessageGroup.GROUP_TYPE, cmd.groupType());

        if (cmd instanceof FinishTxCommand) {
            FinishTxCommand finishTxCommand = (FinishTxCommand) cmd;

            return (T) msgFactory.finishTxCommand()
                    .txId(finishTxCommand.txId())
                    .commit(finishTxCommand.commit())
                    .partitionIds(finishTxCommand.partitionIds())
                    .commitTimestampLong(finishTxCommand.commitTimestampLong())
                    .build();
        } else if (cmd instanceof WriteIntentSwitchCommand) {
            WriteIntentSwitchCommand writeIntentSwitchCommand = (WriteIntentSwitchCommand) cmd;

            return (T) msgFactory.writeIntentSwitchCommand()
                    .txId(writeIntentSwitchCommand.txId())
                    .commit(writeIntentSwitchCommand.commit())
                    .commitTimestampLong(writeIntentSwitchCommand.commitTimestampLong())
                    .build();
        } else if (cmd instanceof UpdateCommand) {
            UpdateCommand updateCommand = (UpdateCommand) cmd;

            return (T) msgFactory.updateCommand()
                    .txId(updateCommand.txId())
                    .rowUuid(updateCommand.rowUuid())
                    .tablePartitionId(updateCommand.tablePartitionId())
                    .messageRowToUpdate(updateCommand.messageRowToUpdate())
                    .txCoordinatorId(updateCommand.txCoordinatorId())
                    .build();
        } else if (cmd instanceof UpdateAllCommand) {
            UpdateAllCommand updateCommand = (UpdateAllCommand) cmd;

            return (T) msgFactory.updateAllCommand()
                    .txId(updateCommand.txId())
                    .messageRowsToUpdate(updateCommand.messageRowsToUpdate())
                    .tablePartitionId(updateCommand.tablePartitionId())
                    .txCoordinatorId(updateCommand.txCoordinatorId())
                    .build();
        } else {
            fail(cmd.toString());

            return null;
        }
    }

    private BinaryRowMessage binaryRowMessage(int id) throws Exception {
        Row row = kvMarshaller.marshal(
                new TestKey(id, String.valueOf(id)),
                new TestValue(id, String.valueOf(id))
        );

        return msgFactory.binaryRowMessage()
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
