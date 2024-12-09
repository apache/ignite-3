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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.server.AbstractKeyValueStorageTest.key;
import static org.apache.ignite.internal.metastorage.server.BasicOperationsKeyValueStorageTest.createCommandId;
import static org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext.kvContext;
import static org.apache.ignite.internal.util.ArrayUtils.INT_EMPTY_ARRAY;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition.Type;

/**
 * Enumeration for testing storage revision change notification. It is expected that there is already an {@link Entry} with key {@code 0}
 * in the storage.
 */
enum UpdateRevisionOperation {
    // Simple operations.
    PUT_NEW(storage -> put(storage, 1)),
    PUT_ALL_NEW(storage -> putAll(storage, 1)),
    PUT_EXISTING(storage -> put(storage, 0)),
    PUT_ALL_EXISTING(storage -> putAll(storage, 0)),
    REMOVE_EXISTING(storage -> remove(storage, 0)),
    REMOVE_ALL_EXISTING(storage -> removeAll(storage, 0)),
    REMOVE_NOT_EXISTING(storage -> remove(storage, 1)),
    REMOVE_ALL_NOT_EXISTING(storage -> removeAll(storage, 1)),
    PUT_ALL_EMPTY(storage -> putAll(storage, INT_EMPTY_ARRAY)),
    REMOVE_ALL_EMPTY(storage -> removeAll(storage, INT_EMPTY_ARRAY)),
    // Invoke operations.
    INVOKE_PUT_NEW(storage -> invokeSuccessCondition(storage, putOperation(1))),
    INVOKE_IF_PUT_NEW(storage -> invokeIfSuccessCondition(storage, putOperation(1))),
    INVOKE_PUT_EXISTING(storage -> invokeSuccessCondition(storage, putOperation(0))),
    INVOKE_IF_PUT_EXISTING(storage -> invokeIfSuccessCondition(storage, putOperation(0))),
    INVOKE_REMOVE_EXISTING(storage -> invokeSuccessCondition(storage, removeOperation(0))),
    INVOKE_IF_REMOVE_EXISTING(storage -> invokeIfSuccessCondition(storage, removeOperation(0))),
    INVOKE_REMOVE_NOT_EXISTING(storage -> invokeSuccessCondition(storage, removeOperation(1))),
    INVOKE_IF_REMOVE_NOT_EXISTING(storage -> invokeIfSuccessCondition(storage, removeOperation(1))),
    INVOKE_NOOP(storage -> invokeSuccessCondition(storage, noop())),
    INVOKE_IF_NOOP(storage -> invokeIfSuccessCondition(storage, noop()));

    private final Consumer<KeyValueStorage> function;

    UpdateRevisionOperation(Consumer<KeyValueStorage> function) {
        this.function = function;
    }

    void execute(KeyValueStorage storage) {
        Entry entry = storage.get(key(0));

        assertFalse(entry.empty());
        assertFalse(entry.tombstone());

        function.accept(storage);
    }

    private static void put(KeyValueStorage storage, int key) {
        storage.put(key(key), value(1), randomKvContext());
    }

    private static void putAll(KeyValueStorage storage, int... keys) {
        storage.putAll(toListKeyByteArray(keys), toListValueByteArray(keys), randomKvContext());
    }

    private static void remove(KeyValueStorage storage, int key) {
        storage.remove(key(key), randomKvContext());
    }

    private static void removeAll(KeyValueStorage storage, int... keys) {
        storage.removeAll(toListKeyByteArray(keys), randomKvContext());
    }

    private static void invokeSuccessCondition(KeyValueStorage storage, Operation operation) {
        storage.invoke(
                new ExistenceCondition(Type.EXISTS, key(0)),
                List.of(operation),
                List.of(noop()),
                randomKvContext(),
                createCommandId()
        );
    }

    private static void invokeIfSuccessCondition(KeyValueStorage storage, Operation operation) {
        storage.invoke(
                new If(
                        new ExistenceCondition(Type.EXISTS, key(0)),
                        new Statement(ops(operation).yield()),
                        new Statement(ops(noop()).yield())
                ),
                randomKvContext(),
                createCommandId()
        );
    }

    private static KeyValueUpdateContext randomKvContext() {
        return kvContext(hybridTimestamp(100_500));
    }

    private static byte[] value(int value) {
        return ByteArray.fromString("value=" + value).bytes();
    }

    private static Operation putOperation(int key) {
        return Operations.put(keyByteArray(key), value(1));
    }

    private static Operation removeOperation(int key) {
        return Operations.remove(keyByteArray(key));
    }

    private static ByteArray keyByteArray(int key) {
        return new ByteArray(key(key));
    }

    private static List<byte[]> toListKeyByteArray(int... keys) {
        return IntStream.of(keys).mapToObj(AbstractKeyValueStorageTest::key).collect(toList());
    }

    private static List<byte[]> toListValueByteArray(int... keys) {
        return IntStream.of(keys).mapToObj(UpdateRevisionOperation::value).collect(toList());
    }
}
