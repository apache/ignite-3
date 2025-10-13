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

package org.apache.ignite.internal.replicator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of replicator raft commands. It is verified that deserialization of commands that
 * were created on earlier versions of the product will be error-free.
 *
 * <p>For MAC users with aarch64 architecture, you will need to add {@code || "aarch64".equals(arch)} to the
 * {@code GridUnsafe#unaligned()} for the tests to pass. For more details, see
 * <a href="https://lists.apache.org/thread/67coyvm8mo7106mkndt24yqwtbvb7590">discussion</a>.</p>
 *
 * <p>To serialize commands, use {@link #serializeAll()} and insert the result into the appropriate tests.</p>
 */
public class ReplicatorCommandsCompatibilityTest extends BaseIgniteAbstractTest {
    private final MessageSerializationRegistry registry = new MessageSerializationRegistryImpl();

    private final Marshaller marshaller = new ThreadLocalOptimizedMarshaller(registry);

    private final ReplicaMessagesFactory factory = new ReplicaMessagesFactory();

    @BeforeEach
    void setUp() {
        new ReplicaMessagesSerializationRegistryInitializer().registerFactories(registry);
    }

    @Test
    void testSafeTimeSyncCommand() {
        SafeTimeSyncCommand command = decodeCommand("CSlH");

        assertEquals(initiatorTime(), command.initiatorTime());
    }

    @Test
    void testPrimaryReplicaChangeCommand() {
        PrimaryReplicaChangeCommand command = decodeCommand("CSorAAAAAAAAAAAqAAAAAAAAAEUGbm9kZTE=");

        assertEquals(42, command.leaseStartTime());
        assertEquals(uuid(), command.primaryReplicaNodeId());
        assertEquals("node1", command.primaryReplicaNodeName());
    }

    private static HybridTimestamp initiatorTime() {
        return HybridTimestamp.hybridTimestamp(70);
    }

    private static UUID uuid() {
        return new UUID(42, 69);
    }

    private <T extends Command> T deserializeCommand(byte[] bytes) {
        return marshaller.unmarshall(bytes);
    }

    private <T extends Command> T decodeCommand(String base64) {
        return deserializeCommand(Base64.getDecoder().decode(base64));
    }

    @SuppressWarnings("unused")
    private void serializeAll() {
        List<Command> commands = List.of(
                createSafeTimeSyncCommand(),
                createPrimaryReplicaChangeCommand()
        );

        for (Command c : commands) {
            log.info(">>>>> Serialized command: [c={}, base64='{}']", c.getClass().getSimpleName(), encodeCommand(c));
        }
    }

    private PrimaryReplicaChangeCommand createPrimaryReplicaChangeCommand() {
        return factory.primaryReplicaChangeCommand()
                .leaseStartTime(42)
                .primaryReplicaNodeId(uuid())
                .primaryReplicaNodeName("node1")
                .build();
    }

    private SafeTimeSyncCommand createSafeTimeSyncCommand() {
        return factory.safeTimeSyncCommand()
                .initiatorTime(initiatorTime())
                .build();
    }

    private byte[] serializeCommand(Command c) {
        return marshaller.marshall(c);
    }

    private String encodeCommand(Command c) {
        return Base64.getEncoder().encodeToString(serializeCommand(c));
    }
}
