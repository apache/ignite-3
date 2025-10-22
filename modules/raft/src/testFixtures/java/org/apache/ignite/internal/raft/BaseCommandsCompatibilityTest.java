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

package org.apache.ignite.internal.raft;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Base64;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for unit testing backward compatibility of raft commands.
 *
 * <p>To serialize commands, use {@link #serializeAll()} and insert the result into the appropriate tests.</p>
 *
 * <p>For MAC users with aarch64 architecture, you will need to add {@code || "aarch64".equals(arch)} to the
 * {@code GridUnsafe#unaligned()} for the tests to pass. For more details, see
 * <a href="https://lists.apache.org/thread/67coyvm8mo7106mkndt24yqwtbvb7590">discussion</a>.</p>
 */
public abstract class BaseCommandsCompatibilityTest extends BaseIgniteAbstractTest {
    private final MessageSerializationRegistry registry = new MessageSerializationRegistryImpl();

    private final Marshaller marshaller = new ThreadLocalOptimizedMarshaller(registry);

    @BeforeEach
    void setUp() {
        initializers().forEach(i -> i.registerFactories(registry));
    }

    /** Returns all {@link MessageSerializationRegistryInitializer} that will be needed for testing. */
    protected abstract Collection<MessageSerializationRegistryInitializer> initializers();

    /** Returns all commands to be serialized. */
    protected abstract Collection<Command> commandsToSerialize();

    @SuppressWarnings("unused")
    private void serializeAll() {
        for (Command c : commandsToSerialize()) {
            log.info("Serialized command: [command={}, base64='{}']", c.getClass().getSimpleName(), encodeCommand(c));
        }
    }

    /** Serializes a raft command and encodes it into Base64 string. */
    private String encodeCommand(Command c) {
        return Base64.getEncoder().encodeToString(serializeCommand(c));
    }

    /** Deserializes a raft command from Base64 string. */
    protected <T extends Command> T decodeCommand(String base64) {
        return deserializeCommand(Base64.getDecoder().decode(base64));
    }

    private byte[] serializeCommand(Command c) {
        return marshaller.marshall(c);
    }

    private <T extends Command> T deserializeCommand(byte[] bytes) {
        return marshaller.unmarshall(bytes);
    }

    protected static UUID uuid() {
        return new UUID(42, 69);
    }

    protected static UUID anotherUuid() {
        return new UUID(-42, -69);
    }

    protected static HybridTimestamp initiatorTime() {
        return HybridTimestamp.hybridTimestamp(70);
    }

    protected static HybridTimestamp safeTime() {
        return HybridTimestamp.hybridTimestamp(69);
    }

    protected static HybridTimestamp commitTimestamp() {
        return HybridTimestamp.hybridTimestamp(71);
    }

    /** Annotation to indicate which raft command is being tested. */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface TestForCommand {
        /** Raft command being tested. */
        Class<? extends Command> value();
    }
}
