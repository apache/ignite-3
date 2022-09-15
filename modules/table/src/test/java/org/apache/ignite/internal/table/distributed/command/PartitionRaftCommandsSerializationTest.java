/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for partition RAFT commands serialization.
 */
public class PartitionRaftCommandsSerializationTest extends IgniteAbstractTest {
    /** Key-value marshaller for tests. */
    protected static KvMarshaller<TestKey, TestValue> kvMarshaller;

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
        UpdateCommand cmd = new UpdateCommand(new RowId(1), binaryRow(1), UUID.randomUUID());

        UpdateCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.getRowId(), readCmd.getRowId());
        assertArrayEquals(cmd.getRow().bytes(), readCmd.getRow().bytes());
    }

    @Test
    public void testRemoveCommand() throws Exception {
        UpdateCommand cmd = new UpdateCommand(new RowId(1), UUID.randomUUID());

        UpdateCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.getRowId(), readCmd.getRowId());
        assertNull(readCmd.getRow());
    }

    @Test
    public void testUpdateAllCommand() throws Exception {
        HashMap<RowId, BinaryRow> rowsToUpdate = new HashMap();

        for (int i = 0; i < 10; i++) {
            rowsToUpdate.put(new RowId(i), binaryRow(i));
        }

        var cmd = new UpdateAllCommand(rowsToUpdate, UUID.randomUUID());

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (Map.Entry<RowId, BinaryRow> entry : cmd.getRowsToUpdate().entrySet()) {
            assertTrue(readCmd.getRowsToUpdate().containsKey(entry.getKey()));

            var readVal = readCmd.getRowsToUpdate().get(entry.getKey());
            var val = entry.getValue();

            assertArrayEquals(val.bytes(), readVal.bytes());
        }
    }

    @Test
    public void testRemoveAllCommand() throws Exception {
        ArrayList<RowId> rowsToRemove = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            rowsToRemove.add(new RowId(i));
        }

        var cmd = new UpdateAllCommand(rowsToRemove, UUID.randomUUID());

        UpdateAllCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());

        for (RowId rowId : cmd.getRowsToUpdate().keySet()) {
            assertTrue(readCmd.getRowsToUpdate().containsKey(rowId));
            assertNull(readCmd.getRowsToUpdate().get(rowId));
        }
    }

    @Test
    public void testTxCleanupCommand() throws Exception {
        HybridClock clock = new HybridClock();

        TxCleanupCommand cmd = new TxCleanupCommand(UUID.randomUUID(), true, clock.now());

        TxCleanupCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.commit(), readCmd.commit());
        assertEquals(cmd.commitTimestamp(), readCmd.commitTimestamp());
    }

    @Test
    public void testFinishTxCommand() throws Exception {
        HybridClock clock = new HybridClock();
        ArrayList<String> grps = new ArrayList<String>(10);

        for (int i = 0; i < 10; i++) {
            grps.add("grp-" + i);
        }

        FinishTxCommand cmd = new FinishTxCommand(UUID.randomUUID(), true, clock.now(), grps);

        FinishTxCommand readCmd = copyCommand(cmd);

        assertEquals(cmd.txId(), readCmd.txId());
        assertEquals(cmd.commit(), readCmd.commit());
        assertEquals(cmd.commitTimestamp(), readCmd.commitTimestamp());
        assertEquals(cmd.replicationGroupIds(), readCmd.replicationGroupIds());
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

    private static BinaryRow binaryRow(int id) throws Exception {
        return kvMarshaller.marshal(new TestKey(id, String.valueOf(id)), new TestValue(id, String.valueOf(id)));
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
