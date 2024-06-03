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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.hlc.TestClockService.TEST_MAX_CLOCK_SKEW_MILLIS;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for idempotency of {@link org.apache.ignite.internal.metastorage.command.IdempotentCommand}.
 */
@ExtendWith(ConfigurationExtension.class)
public class IdempotentCommandCacheTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "node";

    private static final MetaStorageCommandsFactory CMD_FACTORY = new MetaStorageCommandsFactory();

    private KeyValueStorage storage;

    private MetaStorageListener metaStorageListener;

    private final HybridClock clock = new HybridClockImpl();

    @Nullable
    private Serializable lastCommandResult;

    private final CommandIdGenerator commandIdGenerator = new CommandIdGenerator(() -> UUID.randomUUID().toString());

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @BeforeEach
    public void setUp() {
        storage = new SimpleInMemoryKeyValueStorage(NODE_NAME);
        metaStorageListener = new MetaStorageListener(
                storage,
                new ClusterTimeImpl(NODE_NAME, new IgniteSpinBusyLock(), clock),
                completedFuture(() -> TEST_MAX_CLOCK_SKEW_MILLIS),
                raftConfiguration.responseTimeout()
        );
    }

    @Test
    public void testIdempotentInvoke() {
        ByteArray testKey = new ByteArray("key".getBytes(StandardCharsets.UTF_8));
        ByteArray testValue = new ByteArray("value".getBytes(StandardCharsets.UTF_8));
        ByteArray anotherValue = new ByteArray("another".getBytes(StandardCharsets.UTF_8));

        InvokeCommand command = CMD_FACTORY.invokeCommand()
                .id(commandIdGenerator.newId())
                .condition(notExists(testKey))
                .success(List.of(put(testKey, testValue.bytes())))
                .failure(List.of(put(testKey, anotherValue.bytes())))
                .safeTimeLong(clock.now().longValue())
                .initiatorTimeLong(clock.now().longValue())
                .build();

        metaStorageListener.onWrite(commandIterator(command));

        assertNotNull(lastCommandResult);
        assertTrue((Boolean) lastCommandResult);
        checkValueInStorage(testKey.bytes(), testValue.bytes());

        // Another call of same command.
        metaStorageListener.onWrite(commandIterator(command));
        assertNotNull(lastCommandResult);
        assertTrue((Boolean) lastCommandResult);
        checkValueInStorage(testKey.bytes(), testValue.bytes());
    }

    @Test
    public void testIdempotentMultiInvoke() {
        ByteArray testKey = new ByteArray("key".getBytes(StandardCharsets.UTF_8));
        ByteArray testValue = new ByteArray("value".getBytes(StandardCharsets.UTF_8));
        ByteArray anotherValue = new ByteArray("another".getBytes(StandardCharsets.UTF_8));

        Iif iif = iif(
                notExists(testKey),
                ops(put(testKey, testValue.bytes())).yield(true),
                ops(put(testKey, anotherValue.bytes())).yield(false)
        );

        MultiInvokeCommand command = CMD_FACTORY.multiInvokeCommand()
                .id(commandIdGenerator.newId())
                .iif(iif)
                .safeTimeLong(clock.now().longValue())
                .initiatorTimeLong(clock.now().longValue())
                .build();

        metaStorageListener.onWrite(commandIterator(command));

        StatementResult result = (StatementResult) lastCommandResult;
        assertNotNull(result);
        assertTrue(result.getAsBoolean());
        checkValueInStorage(testKey.bytes(), testValue.bytes());

        // Another call of same command.
        metaStorageListener.onWrite(commandIterator(command));
        result = (StatementResult) lastCommandResult;
        assertNotNull(result);
        assertTrue(result.getAsBoolean());
        checkValueInStorage(testKey.bytes(), testValue.bytes());
    }

    @Test
    public void testNonIdempotentCommand() {
        ByteArray testKey = new ByteArray("key".getBytes(StandardCharsets.UTF_8));
        ByteArray testValue0 = new ByteArray("value".getBytes(StandardCharsets.UTF_8));
        ByteArray testValue1 = new ByteArray("value".getBytes(StandardCharsets.UTF_8));

        PutCommand command0 = CMD_FACTORY.putCommand()
                .key(testKey.bytes())
                .value(testValue0.bytes())
                .safeTimeLong(clock.now().longValue())
                .initiatorTimeLong(clock.now().longValue())
                .build();

        metaStorageListener.onWrite(commandIterator(command0));

        assertNull(lastCommandResult);
        checkValueInStorage(testKey.bytes(), testValue0.bytes());

        // Another call of same command.
        metaStorageListener.onWrite(commandIterator(command0));
        assertNull(lastCommandResult);
        checkValueInStorage(testKey.bytes(), testValue0.bytes());

        PutCommand command1 = CMD_FACTORY.putCommand()
                .key(testKey.bytes())
                .value(testValue1.bytes())
                .safeTimeLong(clock.now().longValue())
                .initiatorTimeLong(clock.now().longValue())
                .build();

        metaStorageListener.onWrite(commandIterator(command1));

        assertNull(lastCommandResult);
        checkValueInStorage(testKey.bytes(), testValue1.bytes());
    }

    private void checkValueInStorage(byte[] testKey, byte[] testValueExpected) {
        Entry e = storage.get(testKey);
        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertArrayEquals(testValueExpected, e.value());
    }

    private Iterator<CommandClosure<WriteCommand>> commandIterator(WriteCommand command) {
        List<CommandClosure<WriteCommand>> closureList = List.of(new TestCommandClosure(command));

        return closureList.iterator();
    }

    private class TestCommandClosure implements CommandClosure<WriteCommand> {
        private final WriteCommand command;

        private TestCommandClosure(WriteCommand command) {
            this.command = command;
        }

        @Override
        public WriteCommand command() {
            return command;
        }

        @Override
        public void result(@Nullable Serializable res) {
            lastCommandResult = res;
        }
    }
}
