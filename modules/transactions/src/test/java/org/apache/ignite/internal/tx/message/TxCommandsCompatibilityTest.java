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

package org.apache.ignite.internal.tx.message;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of tx raft commands. It is verified that deserialization of commands that were
 * created on earlier versions of the product will be error-free.
 *
 * <p>For MAC users with aarch64 architecture, you will need to add {@code || "aarch64".equals(arch)} to the
 * {@code GridUnsafe#unaligned()} for the tests to pass. For more details, see
 * <a href="https://lists.apache.org/thread/67coyvm8mo7106mkndt24yqwtbvb7590">discussion</a>.</p>
 *
 * <p>To serialize commands, use {@link #serializeAll()} and insert the result into the appropriate tests.</p>
 */
public class TxCommandsCompatibilityTest extends BaseIgniteAbstractTest {
    private final MessageSerializationRegistry registry = new MessageSerializationRegistryImpl();

    private final Marshaller marshaller = new ThreadLocalOptimizedMarshaller(registry);

    private final TxMessagesFactory factory = new TxMessagesFactory();

    @BeforeEach
    void setUp() {
        new TxMessagesSerializationRegistryInitializer().registerFactories(registry);
    }

    @Test
    void testVacuumTxStatesCommand() {
        VacuumTxStatesCommand command = decodeCommand("Bg4CAAAAAAAAAAAqAAAAAAAAAEU=");

        assertEquals(Set.of(uuid()), command.txIds());
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
                createVacuumTxStatesCommand()
        );

        for (Command c : commands) {
            log.info(">>>>> Serialized command: [c={}, base64='{}']", c.getClass().getSimpleName(), encodeCommand(c));
        }
    }

    private VacuumTxStatesCommand createVacuumTxStatesCommand() {
        return factory.vacuumTxStatesCommand()
                .txIds(Set.of(uuid()))
                .build();
    }

    private byte[] serializeCommand(Command c) {
        return marshaller.marshall(c);
    }

    private String encodeCommand(Command c) {
        return Base64.getEncoder().encodeToString(serializeCommand(c));
    }
}
