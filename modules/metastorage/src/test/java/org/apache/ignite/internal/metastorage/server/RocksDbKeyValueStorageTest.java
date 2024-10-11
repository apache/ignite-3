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

package org.apache.ignite.internal.metastorage.server;

import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.server.ExistenceCondition.Type.NOT_EXISTS;
import static org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler.IDEMPOTENT_COMMAND_PREFIX;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.UUID;
import java.util.zip.Checksum;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.impl.CommandIdGenerator;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.raft.jraft.util.CRC64;
import org.junit.jupiter.api.Test;

/**
 * Tests for RocksDB key-value storage implementation.
 */
public class RocksDbKeyValueStorageTest extends BasicOperationsKeyValueStorageTest {
    @Override
    public KeyValueStorage createStorage() {
        return new RocksDbKeyValueStorage("test", workDir.resolve("storage"), new NoOpFailureManager());
    }

    @Override
    protected boolean supportsChecksums() {
        return true;
    }

    @Test
    public void putChecksum() {
        byte[] key = key(1);
        byte[] val = keyValue(1, 1);

        putToMs(key, val);
        long checksum1 = storage.checksum(1);

        assertThat(checksum1, is(checksum(
                longToBytes(0), // prev checksum
                bytes(1), // PUT
                intToBytes(key.length), key,
                intToBytes(val.length), val
        )));

        // Repeating the same command, the checksum must be different.
        putToMs(key, val);
        assertThat(storage.checksum(2), is(checksum(
                longToBytes(checksum1),
                bytes(1),
                intToBytes(key.length), key,
                intToBytes(val.length), val
        )));
    }

    @Test
    public void putAllChecksum() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);
        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        putAllToMs(List.of(key1, key2), List.of(val1, val2));
        long checksum1 = storage.checksum(1);

        assertThat(checksum1, is(checksum(
                longToBytes(0), // prev checksum
                bytes(2), // PUT_ALL
                intToBytes(2), // entry count
                intToBytes(key1.length), key1,
                intToBytes(val1.length), val1,
                intToBytes(key2.length), key2,
                intToBytes(val2.length), val2
        )));

        // Repeating the same command, the checksum must be different.
        putAllToMs(List.of(key1, key2), List.of(val1, val2));
        assertThat(storage.checksum(2), is(checksum(
                longToBytes(checksum1),
                bytes(2), // PUT_ALL
                intToBytes(2), // entry count
                intToBytes(key1.length), key1,
                intToBytes(val1.length), val1,
                intToBytes(key2.length), key2,
                intToBytes(val2.length), val2
        )));
    }

    @Test
    public void removeChecksum() {
        byte[] key = key(1);
        byte[] val = keyValue(1, 1);

        putToMs(key, val);
        long checksum1 = storage.checksum(1);

        removeFromMs(key);
        long checksum2 = storage.checksum(2);
        assertThat(checksum2, is(checksum(
                longToBytes(checksum1),
                bytes(3), // REMOVE
                intToBytes(key.length), key
        )));

        // Repeating the same command, the checksum must be different.
        removeFromMs(key);
        assertThat(storage.checksum(3), is(checksum(
                longToBytes(checksum2),
                bytes(3), // REMOVE
                intToBytes(key.length), key
        )));
    }

    @Test
    public void removeAllChecksum() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);
        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        putAllToMs(List.of(key1, key2), List.of(val1, val2));
        long checksum1 = storage.checksum(1);

        removeAllFromMs(List.of(key1, key2));
        long checksum2 = storage.checksum(2);
        assertThat(checksum2, is(checksum(
                longToBytes(checksum1),
                bytes(4), // REMOVE_ALL
                intToBytes(2), // key count
                intToBytes(key1.length), key1,
                intToBytes(key2.length), key2
        )));

        // Repeating the same command, the checksum must be different.
        removeAllFromMs(List.of(key1, key2));
        assertThat(storage.checksum(3), is(checksum(
                longToBytes(checksum2),
                bytes(4), // REMOVE_ALL
                intToBytes(2), // key count
                intToBytes(key1.length), key1,
                intToBytes(key2.length), key2
        )));
    }

    @Test
    public void singleInvokeChecksum() {
        byte[] key = key(1);
        byte[] val = keyValue(1, 1);
        CommandIdGenerator commandIdGenerator = new CommandIdGenerator(() -> new UUID(1, 2));

        ExistenceCondition condition = new ExistenceCondition(NOT_EXISTS, key);
        List<Operation> successfulBranch = List.of(put(new ByteArray(key), val));
        List<Operation> failureBranch = List.of(remove(new ByteArray(key)));

        CommandId commandId1 = commandIdGenerator.newId();
        invokeOnMs(condition, successfulBranch, failureBranch, commandId1);

        long checksum1 = storage.checksum(1);

        byte[] idempotentCommandPutKey1 = idempotentCommandPutKey(commandId1);
        byte[] updateResult1 = KeyValueStorage.INVOKE_RESULT_TRUE_BYTES;
        assertThat(checksum1, is(checksum(
                longToBytes(0), // prev checksum
                bytes(5), // SINGLE_INVOKE
                intToBytes(updateResult1.length), updateResult1, // successful branch
                intToBytes(2), // op count (as there is also a system command)
                bytes(1), // PUT
                intToBytes(key.length), key,
                intToBytes(val.length), val,
                bytes(1), // PUT
                intToBytes(idempotentCommandPutKey1.length), idempotentCommandPutKey1,
                intToBytes(updateResult1.length), updateResult1
        )));

        // Repeating the same command, but it now executes another branch.
        CommandId commandId2 = commandIdGenerator.newId();
        invokeOnMs(condition, successfulBranch, failureBranch, commandId2);

        long checksum2 = storage.checksum(2);

        byte[] idempotentCommandPutKey2 = idempotentCommandPutKey(commandId2);
        byte[] updateResult2 = KeyValueStorage.INVOKE_RESULT_FALSE_BYTES;
        assertThat(checksum2, is(checksum(
                longToBytes(checksum1),
                bytes(5), // SINGLE_INVOKE
                intToBytes(updateResult2.length), updateResult2, // failure branch
                intToBytes(2), // op count (as there is also a system command)
                bytes(3), // REMOVE
                intToBytes(key.length), key,
                bytes(1), // PUT
                intToBytes(idempotentCommandPutKey2.length), idempotentCommandPutKey2,
                intToBytes(updateResult2.length), updateResult2
        )));
    }

    private static byte[] idempotentCommandPutKey(CommandId commandId) {
        return new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString()).bytes();
    }

    @Test
    public void multiInvokeChecksum() {
        byte[] key = key(1);
        byte[] val = keyValue(1, 1);
        CommandIdGenerator commandIdGenerator = new CommandIdGenerator(() -> new UUID(1, 2));

        If iif = new If(
                new ExistenceCondition(NOT_EXISTS, key),
                new Statement(ops(put(new ByteArray(key), val)).yield(1)),
                new Statement(ops(remove(new ByteArray(key))).yield(2))
        );

        CommandId commandId1 = commandIdGenerator.newId();
        invokeOnMs(iif, commandId1);

        long checksum1 = storage.checksum(1);

        byte[] idempotentCommandPutKey1 = idempotentCommandPutKey(commandId1);
        byte[] updateResult1 = intToBytes(1);
        assertThat(checksum1, is(checksum(
                longToBytes(0), // prev checksum
                bytes(6), // MULTI_INVOKE
                intToBytes(updateResult1.length), updateResult1, // successful branch
                intToBytes(2), // op count (as there is also a system command)
                bytes(1), // PUT
                intToBytes(key.length), key,
                intToBytes(val.length), val,
                bytes(1), // PUT
                intToBytes(idempotentCommandPutKey1.length), idempotentCommandPutKey1,
                intToBytes(updateResult1.length), updateResult1
        )));

        // Repeating the same command, but it now executes another branch.
        CommandId commandId2 = commandIdGenerator.newId();
        invokeOnMs(iif, commandId2);

        long checksum2 = storage.checksum(2);

        byte[] idempotentCommandPutKey2 = idempotentCommandPutKey(commandId2);
        byte[] updateResult2 = intToBytes(2);
        assertThat(checksum2, is(checksum(
                longToBytes(checksum1),
                bytes(6), // MULTI_INVOKE
                intToBytes(updateResult2.length), updateResult2, // failure branch
                intToBytes(2), // op count (as there is also a system command)
                bytes(3), // REMOVE
                intToBytes(key.length), key,
                bytes(1), // PUT
                intToBytes(idempotentCommandPutKey2.length), idempotentCommandPutKey2,
                intToBytes(updateResult2.length), updateResult2
        )));
    }

    private static byte[] bytes(int... ints) {
        byte[] bytes = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            //noinspection NumericCastThatLosesPrecision
            bytes[i] = (byte) ints[i];
        }
        return bytes;
    }

    private static long checksum(byte[]... arrays) {
        Checksum checksum = new CRC64();

        for (byte[] array : arrays) {
            checksum.update(array);
        }

        return checksum.getValue();
    }
}
