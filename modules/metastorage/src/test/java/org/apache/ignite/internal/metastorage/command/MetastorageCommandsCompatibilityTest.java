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
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.Statements;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of metastorage raft commands. It is verified that deserialization of commands
 * that were created on earlier versions of the product will be error-free.
 *
 * <p> For MAC users with aarch64 architecture, you will need to add {@code || "aarch64".equals(arch)} to the
 * {@code GridUnsafe#unaligned()} for the tests to pass.</p>
 */
public class MetastorageCommandsCompatibilityTest extends BaseIgniteAbstractTest {
    private static final MessageSerializationRegistry REGISTRY = new MessageSerializationRegistryImpl();

    private static final Marshaller MARSHALLER = new ThreadLocalOptimizedMarshaller(REGISTRY);

    private static final MetaStorageCommandsFactory FACTORY = new MetaStorageCommandsFactory();

    @BeforeAll
    static void beforeAll() {
        new MetaStorageCommandsSerializationRegistryInitializer().registerFactories(REGISTRY);
        new MetaStorageMessagesSerializationRegistryInitializer().registerFactories(REGISTRY);
    }

    @Test
    void testCompactionCommand() {
        CompactionCommand command = decodeCommand("cEkrR0Y=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(42, command.compactionRevision());
    }

    @Test
    void testEvictIdempotentCommandsCacheCommand() {
        EvictIdempotentCommandsCacheCommand command = decodeCommand("cEhlR0Y=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(HybridTimestamp.hybridTimestamp(100), command.evictionTimestamp());
    }

    @Test
    void testGetAllCommand() {
        GetAllCommand command = decodeCommand("cB8DAwVrZXkxAwVrZXkyKw==");

        assertEquals(42, command.revision());
        assertEquals(List.of(key("key1"), key("key2")), command.keys());
    }

    @Test
    void testGetChecksumCommand() {
        GetChecksumCommand command = decodeCommand("cCMr");

        assertEquals(42, command.revision());
    }

    @Test
    void testGetCommand() {
        GetCommand command = decodeCommand("cBUDBWtleTEr");

        assertEquals(42, command.revision());
        assertEquals(key("key1"), command.key());
    }

    @Test
    void testGetCurrentRevisionsCommand() {
        Command command = decodeCommand("cCI=");

        assertInstanceOf(GetCurrentRevisionsCommand.class, command);
    }

    @Test
    void testGetPrefixCommand() {
        GetPrefixCommand command = decodeCommand("cD5lAQMIcHJlZml4MQ1wcmV2aW91c0tleTEr");

        assertEquals(key("prefix1"), command.prefix());
        assertEquals(100, command.batchSize());
        assertTrue(command.includeTombstones());
        assertArrayEquals(keyBytes("previousKey1"), command.previousKey());
        assertEquals(42, command.revUpperBound());
    }

    @Test
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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26664")
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
    void testPutAllCommand() {
        PutAllCommand command = decodeCommand("cDNHAwMFa2V5MQMFa2V5MkYDAwV2YWwxAwV2YWwy");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(List.of(key("key1"), key("key2")), command.keys());
        assertEquals(List.of(key("val1"), key("val2")), command.values());
    }

    @Test
    void testPutCommand() {
        PutCommand command = decodeCommand("cClHAwVrZXkxRgMFdmFsMQ==");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(key("key1"), command.key());
        assertEquals(key("val1"), command.value());
    }

    @Test
    void testRemoveAllCommand() {
        RemoveAllCommand command = decodeCommand("cDRHAwMFa2V5MQMFa2V5MkY=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(List.of(key("key1"), key("key2")), command.keys());
    }

    @Test
    void testRemoveByPrefixCommand() {
        RemoveByPrefixCommand command = decodeCommand("cDVHAwhwcmVmaXgxRg==");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(key("prefix1"), command.prefix());
    }

    @Test
    void testRemoveCommand() {
        RemoveCommand command = decodeCommand("cCpHAwVrZXkxRg==");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(key("key1"), command.key());
    }

    @Test
    void testSyncTimeCommand() {
        SyncTimeCommand command = decodeCommand("cEcrR0Y=");

        assertEquals(safeTime(), command.safeTime());
        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(42, command.initiatorTerm());
    }

    private static HybridTimestamp initiatorTime() {
        return HybridTimestamp.hybridTimestamp(70);
    }

    private static HybridTimestamp safeTime() {
        return HybridTimestamp.hybridTimestamp(69);
    }

    private static UUID uuid() {
        return new UUID(42, 69);
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

    private static <T extends Command> T deserializeCommand(byte[] bytes) {
        return MARSHALLER.unmarshall(bytes);
    }

    private static <T extends Command> T decodeCommand(String base64) {
        return deserializeCommand(Base64.getDecoder().decode(base64));
    }

    @SuppressWarnings("unused")
    void serializeAll() {
        List<Command> commands = List.of(
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

        for (Command c : commands) {
            log.info(">>>>> Serialized command: [c={}, base64='{}']", c.getClass().getSimpleName(), encodeCommand(c));
        }
    }

    private static SyncTimeCommand createSyncTimeCommand() {
        return FACTORY.syncTimeCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .initiatorTerm(42)
                .build();
    }

    private static RemoveCommand createRemoveCommand() {
        return FACTORY.removeCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .key(key("key1"))
                .build();
    }

    private static RemoveByPrefixCommand createRemoveByPrefixCommand() {
        return FACTORY.removeByPrefixCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .prefix(key("prefix1"))
                .build();
    }

    private static RemoveAllCommand createRemoveAllCommand() {
        return FACTORY.removeAllCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .keys(List.of(key("key1"), key("key2")))
                .build();
    }

    private static PutCommand createPutCommand() {
        return FACTORY.putCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .key(key("key1"))
                .value(key("val1"))
                .build();
    }

    private static PutAllCommand createPutAllCommand() {
        return FACTORY.putAllCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .keys(List.of(key("key1"), key("key2")))
                .values(List.of(key("val1"), key("val2")))
                .build();
    }

    private static MultiInvokeCommand createMultiInvokeCommand() {
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

        return FACTORY.multiInvokeCommand()
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

    private static InvokeCommand createInvokeCommand() {
        return FACTORY.invokeCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .id(commandId())
                .condition(Conditions.exists(keyAsByteArray("exists1")))
                .success(List.of(Operations.put(keyAsByteArray("put1"), keyBytes("val1"))))
                .failure(List.of(Operations.remove(keyAsByteArray("remove1"))))
                .build();
    }

    private static GetRangeCommand createGetRangeCommand() {
        return FACTORY.getRangeCommand()
                .keyFrom(key("keyFrom1"))
                .keyTo(key("keyTo1"))
                .batchSize(100)
                .includeTombstones(true)
                .previousKey(keyBytes("previousKey1"))
                .revUpperBound(42)
                .build();
    }

    private static GetPrefixCommand createGetPrefixCommand() {
        return FACTORY.getPrefixCommand()
                .prefix(key("prefix1"))
                .batchSize(100)
                .includeTombstones(true)
                .previousKey(keyBytes("previousKey1"))
                .revUpperBound(42)
                .build();
    }

    private static GetCurrentRevisionsCommand createGetCurrentRevisionsCommand() {
        return FACTORY.getCurrentRevisionsCommand()
                .build();
    }

    private static GetCommand createGetCommand() {
        return FACTORY.getCommand()
                .revision(42)
                .key(key("key1"))
                .build();
    }

    private static GetChecksumCommand createGetChecksumCommand() {
        return FACTORY.getChecksumCommand()
                .revision(42)
                .build();
    }

    private static GetAllCommand createGetAllCommand() {
        return FACTORY.getAllCommand()
                .revision(42)
                .keys(List.of(key("key1"), key("key2")))
                .build();
    }

    private static EvictIdempotentCommandsCacheCommand createEvictIdempotentCommandsCacheCommand() {
        return FACTORY.evictIdempotentCommandsCacheCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .evictionTimestamp(HybridTimestamp.hybridTimestamp(100))
                .build();
    }

    private static CompactionCommand createCompactionCommand() {
        return FACTORY.compactionCommand()
                .safeTime(safeTime())
                .initiatorTime(initiatorTime())
                .compactionRevision(42)
                .build();
    }

    private static byte[] serializeCommand(Command c) {
        return MARSHALLER.marshall(c);
    }

    private static String encodeCommand(Command c) {
        return Base64.getEncoder().encodeToString(serializeCommand(c));
    }
}
