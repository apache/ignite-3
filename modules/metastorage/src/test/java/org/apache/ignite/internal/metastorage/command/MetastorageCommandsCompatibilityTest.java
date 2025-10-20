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

package org.apache.ignite.internal.metastorage.command;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.Statements;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest;
import org.apache.ignite.internal.raft.Command;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of metastorage raft commands. It is verified that deserialization of commands
 * that were created on earlier versions of the product will be error-free.
 */
public class MetastorageCommandsCompatibilityTest extends BaseCommandsCompatibilityTest {
    private final MetaStorageCommandsFactory factory = new MetaStorageCommandsFactory();

    @Override
    protected Collection<MessageSerializationRegistryInitializer> initializers() {
        return List.of(
                new MetaStorageCommandsSerializationRegistryInitializer(),
                new MetaStorageMessagesSerializationRegistryInitializer()
        );
    }

    @Override
    protected Collection<Command> commandsToSerialize() {
        return List.of(
                createCompactionCommand(),
                createEvictIdempotentCommandsCacheCommand(),
                createGetAllCommand(),
                createGetChecksumCommand(),
                createGetCommand(),
                createGetCurrentRevisionsCommand(),
                createGetPrefixCommand(),
                createGetRangeCommand(),
                createInvokeCommand(),
                createMultiInvokeCommand(),
                createPutAllCommand(),
                createPutCommand(),
                createRemoveAllCommand(),
                createRemoveByPrefixCommand(),
                createRemoveCommand(),
                createSyncTimeCommand()
        );
    }

    @Test
    @TestForCommand(CompactionCommand.class)
    void testCompactionCommand() {
        CompactionCommand command = decodeCommand("cEkrR0Y=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(42, command.compactionRevision());
    }

    @Test
    @TestForCommand(EvictIdempotentCommandsCacheCommand.class)
    void testEvictIdempotentCommandsCacheCommand() {
        EvictIdempotentCommandsCacheCommand command = decodeCommand("cEhlR0Y=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(HybridTimestamp.hybridTimestamp(100), command.evictionTimestamp());
    }

    @Test
    @TestForCommand(GetAllCommand.class)
    void testGetAllCommand() {
        GetAllCommand command = decodeCommand("cB8DAwVrZXkxAwVrZXkyKw==");

        assertEquals(42, command.revision());
        assertEquals(List.of(key("key1"), key("key2")), command.keys());
    }

    @Test
    @TestForCommand(GetChecksumCommand.class)
    void testGetChecksumCommand() {
        GetChecksumCommand command = decodeCommand("cCMr");

        assertEquals(42, command.revision());
    }

    @Test
    @TestForCommand(GetCommand.class)
    void testGetCommand() {
        GetCommand command = decodeCommand("cBUDBWtleTEr");

        assertEquals(42, command.revision());
        assertEquals(key("key1"), command.key());
    }

    @Test
    @TestForCommand(GetCurrentRevisionsCommand.class)
    void testGetCurrentRevisionsCommand() {
        Command command = decodeCommand("cCI=");

        assertInstanceOf(GetCurrentRevisionsCommand.class, command);
    }

    @Test
    @TestForCommand(GetPrefixCommand.class)
    void testGetPrefixCommand() {
        GetPrefixCommand command = decodeCommand("cD5lAQMIcHJlZml4MQ1wcmV2aW91c0tleTEr");

        assertEquals(key("prefix1"), command.prefix());
        assertEquals(100, command.batchSize());
        assertTrue(command.includeTombstones());
        assertArrayEquals(keyBytes("previousKey1"), command.previousKey());
        assertEquals(42, command.revUpperBound());
    }

    @Test
    @TestForCommand(GetRangeCommand.class)
    void testGetRangeCommand() {
        GetRangeCommand command = decodeCommand("cD1lAQMJa2V5RnJvbTEDB2tleVRvMQ1wcmV2aW91c0tleTEr");

        assertEquals(key("keyFrom1"), command.keyFrom());
        assertEquals(key("keyTo1"), command.keyTo());
        assertEquals(100, command.batchSize());
        assertTrue(command.includeTombstones());
        assertArrayEquals(keyBytes("previousKey1"), command.previousKey());
        assertEquals(42, command.revUpperBound());
    }

    @Test
    @TestForCommand(InvokeCommand.class)
    void testInvokeCommand() {
        InvokeCommand command = decodeCommand(
                "cAvfAQIDCGV4aXN0czEOAt8BBgMIcmVtb3ZlMQQA3wEMRwAAAAAAAAAAKgAAAAAAAABFR0YC3wEGAwVwdXQxAwMFdmFsMQ=="
        );

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(commandId(), command.id());
        assertEquals(Conditions.exists(keyAsByteArray("exists1")), command.condition());
        assertEquals(
                List.of(Operations.put(keyAsByteArray("put1"), keyBytes("val1"))),
                command.success()
        );
        assertEquals(
                List.of(Operations.remove(keyAsByteArray("remove1"))),
                command.failure()
        );
    }

    @Test
    @TestForCommand(MultiInvokeCommand.class)
    void testMultiInvokeCommand() {
        MultiInvokeCommand command = decodeCommand(
                "cAzfAQxHAAAAAAAAAAAqAAAAAAAAAEXfAQnfAQvfAQgC3wEGAwVwdXQxAwMFdmFsMd8BBwMCAd8BBd8BBd8BAgMLdG9tYnN0b25lMRDfAQIDDm5vdFRvbWJzd"
                        + "G9uZTERAt8BBd8BBd8BAgMIZXhpc3RzMQ7fAQIDC25vdEV4aXN0czEPA98BBd8BAwMKcmV2aXNpb24xAgLfAQQDB3ZhbHVlMQkDBXZhbDECAwLf"
                        + "AQvfAQgC3wEGAAIA3wEHAwIAR0Y="
        );

        Condition tombstonesConditions = Conditions.and(
                Conditions.tombstone(keyAsByteArray("tombstone1")),
                Conditions.notTombstone(keyAsByteArray("notTombstone1"))
        );

        Condition existsConditions = Conditions.or(
                Conditions.exists(keyAsByteArray("exists1")),
                Conditions.notExists(keyAsByteArray("notExists1")
                ));

        Condition revisionAndValueConsitions = Conditions.and(
                Conditions.revision(keyAsByteArray("revision1")).eq(1L),
                Conditions.value(keyAsByteArray("value1")).ne(keyBytes("val1"))
        );

        Condition complexCondition = Conditions.and(
                tombstonesConditions,
                Conditions.or(existsConditions, revisionAndValueConsitions)
        );

        Iif iif = Statements.iif(
                complexCondition,
                Operations.ops(Operations.put(keyAsByteArray("put1"), keyBytes("val1"))).yield(true),
                Operations.ops(Operations.noop()).yield(false)
        );

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(commandId(), command.id());
        assertEquals(iif, command.iif());
    }

    @Test
    @TestForCommand(PutAllCommand.class)
    void testPutAllCommand() {
        PutAllCommand command = decodeCommand("cDNHAwMFa2V5MQMFa2V5MkYDAwV2YWwxAwV2YWwy");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(List.of(key("key1"), key("key2")), command.keys());
        assertEquals(List.of(key("val1"), key("val2")), command.values());
    }

    @Test
    @TestForCommand(PutCommand.class)
    void testPutCommand() {
        PutCommand command = decodeCommand("cClHAwVrZXkxRgMFdmFsMQ==");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(key("key1"), command.key());
        assertEquals(key("val1"), command.value());
    }

    @Test
    @TestForCommand(RemoveAllCommand.class)
    void testRemoveAllCommand() {
        RemoveAllCommand command = decodeCommand("cDRHAwMFa2V5MQMFa2V5MkY=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(List.of(key("key1"), key("key2")), command.keys());
    }

    @Test
    @TestForCommand(RemoveByPrefixCommand.class)
    void testRemoveByPrefixCommand() {
        RemoveByPrefixCommand command = decodeCommand("cDVHAwhwcmVmaXgxRg==");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(key("prefix1"), command.prefix());
    }

    @Test
    @TestForCommand(RemoveCommand.class)
    void testRemoveCommand() {
        RemoveCommand command = decodeCommand("cCpHAwVrZXkxRg==");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(key("key1"), command.key());
    }

    @Test
    @TestForCommand(SyncTimeCommand.class)
    void testSyncTimeCommand() {
        SyncTimeCommand command = decodeCommand("cEcrR0Y=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(42, command.initiatorTerm());
    }

    private static CommandId commandId() {
        return CommandId.fromString(uuid() + "_cnt_" + 70);
    }

    private static ByteBuffer key(String key) {
        return ByteBuffer.wrap(keyBytes(key));
    }

    private static byte[] keyBytes(String key) {
        return key.getBytes(UTF_8);
    }

    private static ByteArray keyAsByteArray(String key) {
        return ByteArray.fromString(key);
    }

    private SyncTimeCommand createSyncTimeCommand() {
        return factory.syncTimeCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .initiatorTerm(42)
                .build();
    }

    private RemoveCommand createRemoveCommand() {
        return factory.removeCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .key(key("key1"))
                .build();
    }

    private RemoveByPrefixCommand createRemoveByPrefixCommand() {
        return factory.removeByPrefixCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .prefix(key("prefix1"))
                .build();
    }

    private RemoveAllCommand createRemoveAllCommand() {
        return factory.removeAllCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .keys(List.of(key("key1"), key("key2")))
                .build();
    }

    private PutCommand createPutCommand() {
        return factory.putCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .key(key("key1"))
                .value(key("val1"))
                .build();
    }

    private PutAllCommand createPutAllCommand() {
        return factory.putAllCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .keys(List.of(key("key1"), key("key2")))
                .values(List.of(key("val1"), key("val2")))
                .build();
    }

    private MultiInvokeCommand createMultiInvokeCommand() {
        Condition tombstonesConditions = Conditions.and(
                Conditions.tombstone(keyAsByteArray("tombstone1")),
                Conditions.notTombstone(keyAsByteArray("notTombstone1"))
        );

        Condition existsConditions = Conditions.or(
                Conditions.exists(keyAsByteArray("exists1")),
                Conditions.notExists(keyAsByteArray("notExists1")
                ));

        Condition revisionAndValueConsitions = Conditions.and(
                Conditions.revision(keyAsByteArray("revision1")).eq(1L),
                Conditions.value(keyAsByteArray("value1")).ne(keyBytes("val1"))
        );

        Condition complexCondition = Conditions.and(
                tombstonesConditions,
                Conditions.or(existsConditions, revisionAndValueConsitions)
        );

        return factory.multiInvokeCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .id(commandId())
                .iif(Statements.iif(
                        complexCondition,
                        Operations.ops(Operations.put(keyAsByteArray("put1"), keyBytes("val1"))).yield(true),
                        Operations.ops(Operations.noop()).yield(false)
                ))
                .build();
    }

    private InvokeCommand createInvokeCommand() {
        return factory.invokeCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .id(commandId())
                .condition(Conditions.exists(keyAsByteArray("exists1")))
                .success(List.of(Operations.put(keyAsByteArray("put1"), keyBytes("val1"))))
                .failure(List.of(Operations.remove(keyAsByteArray("remove1"))))
                .build();
    }

    private GetRangeCommand createGetRangeCommand() {
        return factory.getRangeCommand()
                .keyFrom(key("keyFrom1"))
                .keyTo(key("keyTo1"))
                .batchSize(100)
                .includeTombstones(true)
                .previousKey(keyBytes("previousKey1"))
                .revUpperBound(42)
                .build();
    }

    private GetPrefixCommand createGetPrefixCommand() {
        return factory.getPrefixCommand()
                .prefix(key("prefix1"))
                .batchSize(100)
                .includeTombstones(true)
                .previousKey(keyBytes("previousKey1"))
                .revUpperBound(42)
                .build();
    }

    private GetCurrentRevisionsCommand createGetCurrentRevisionsCommand() {
        return factory.getCurrentRevisionsCommand()
                .build();
    }

    private GetCommand createGetCommand() {
        return factory.getCommand()
                .revision(42)
                .key(key("key1"))
                .build();
    }

    private GetChecksumCommand createGetChecksumCommand() {
        return factory.getChecksumCommand()
                .revision(42)
                .build();
    }

    private GetAllCommand createGetAllCommand() {
        return factory.getAllCommand()
                .revision(42)
                .keys(List.of(key("key1"), key("key2")))
                .build();
    }

    private EvictIdempotentCommandsCacheCommand createEvictIdempotentCommandsCacheCommand() {
        return factory.evictIdempotentCommandsCacheCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .evictionTimestamp(HybridTimestamp.hybridTimestamp(100))
                .build();
    }

    private CompactionCommand createCompactionCommand() {
        return factory.compactionCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .compactionRevision(42)
                .build();
    }
}
