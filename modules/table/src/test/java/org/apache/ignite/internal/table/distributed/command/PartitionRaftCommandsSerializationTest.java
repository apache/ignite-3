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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for partition RAFT commands serialization.
 */
public class PartitionRaftCommandsSerializationTest extends IgniteAbstractTest {
    /** Key-value marshaller for tests. */
    protected static KvMarshaller<TestKey, TestValue> kvMarshaller;

    /** Message factory to create messages - RAFT commands. */
    private TableMessagesFactory msgFactory = new TableMessagesFactory();

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
                        .tableId(UUID.randomUUID())
                        .partitionId(1)
                        .build()
                )
                .rowUuid(Timestamp.nextVersion().toUuid())
                .rowBuffer(byteStrFromBinaryRow(1))
                .txId(UUID.randomUUID())
                .build();

        UpdateCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.rowUuid(), readCmd.rowUuid());
        assertArrayEquals(cmd.rowBuffer().toByteArray(), readCmd.rowBuffer().toByteArray());
    }

    @Test
    public void testRemoveCommand() throws Exception {
        UpdateCommand cmd = msgFactory.updateCommand()
                .tablePartitionId(msgFactory.tablePartitionIdMessage()
                        .tableId(UUID.randomUUID())
                        .partitionId(1)
                        .build()
                )
                .rowUuid(Timestamp.nextVersion().toUuid())
                .txId(UUID.randomUUID())
                .build();

        UpdateCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.rowUuid(), readCmd.rowUuid());
        assertNull(readCmd.rowBuffer());
    }

    @Test
    public void testUpdateAllCommand() throws Exception {
        HashMap<UUID, ByteString> rowsToUpdate = new HashMap();

        for (int i = 0; i < 10; i++) {
            rowsToUpdate.put(Timestamp.nextVersion().toUuid(), byteStrFromBinaryRow(i));
        }

        var cmd = msgFactory.updateAllCommand()
                .tablePartitionId(msgFactory.tablePartitionIdMessage()
                        .tableId(UUID.randomUUID())
                        .partitionId(1)
                        .build()
                )
                .rowsToUpdate(rowsToUpdate)
                .txId(UUID.randomUUID())
                .build();

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (Map.Entry<UUID, ByteString> entry : cmd.rowsToUpdate().entrySet()) {
            assertTrue(readCmd.rowsToUpdate().containsKey(entry.getKey()));

            var readVal = readCmd.rowsToUpdate().get(entry.getKey());
            var val = entry.getValue();

            assertArrayEquals(val.toByteArray(), readVal.toByteArray());
        }
    }

    @Test
    public void testRemoveAllCommand() throws Exception {
        Map<UUID, ByteString> rowsToRemove = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            rowsToRemove.put(Timestamp.nextVersion().toUuid(), null);
        }

        var cmd = msgFactory.updateAllCommand()
                .tablePartitionId(msgFactory.tablePartitionIdMessage()
                        .tableId(UUID.randomUUID())
                        .partitionId(1)
                        .build()
                )
                .rowsToUpdate(rowsToRemove)
                .txId(UUID.randomUUID())
                .build();

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (UUID uuid : cmd.rowsToUpdate().keySet()) {
            assertTrue(readCmd.rowsToUpdate().containsKey(uuid));
            assertNull(readCmd.rowsToUpdate().get(uuid));
        }
    }

    @Test
    public void testTxCleanupCommand() throws Exception {
        HybridClock clock = new HybridClockImpl();

        TxCleanupCommand cmd = msgFactory.txCleanupCommand()
                .txId(UUID.randomUUID())
                .commit(true)
                .commitTimestamp(hybridTimestampMessage(clock.now()))
                .build();

        TxCleanupCommand readCmd = copyCommand(cmd);

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
                    .tableId(UUID.randomUUID())
                    .partitionId(i)
                    .build());
        }

        FinishTxCommand cmd = msgFactory.finishTxCommand()
                .txId(UUID.randomUUID())
                .commit(true)
                .commitTimestamp(hybridTimestampMessage(clock.now()))
                .tablePartitionIds(grps)
                .build();

        FinishTxCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.commit(), readCmd.commit());
        assertEquals(cmd.commitTimestamp(), readCmd.commitTimestamp());
        assertEquals(cmd.tablePartitionIds(), readCmd.tablePartitionIds());
    }

    private HybridTimestampMessage hybridTimestampMessage(HybridTimestamp tmstmp) {
        return msgFactory.hybridTimestampMessage()
                .logical(tmstmp.getLogical())
                .physical(tmstmp.getPhysical())
                .build();
    }

    private <T> T copyCommand(T cmd) throws Exception {
        return cmdFromBytes(cmdToBytes(cmd));
    }

    private <T> T cmdFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                return (T) ois.readObject();
            }
        }
    }

    private <T> byte[] cmdToBytes(T cmd) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(cmd);
            }

            baos.flush();

            return baos.toByteArray();
        }
    }

    private static ByteString byteStrFromBinaryRow(int id) throws Exception {
        return new ByteString(kvMarshaller.marshal(new TestKey(id, String.valueOf(id)), new TestValue(id, String.valueOf(id)))
                .byteBuffer());
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
