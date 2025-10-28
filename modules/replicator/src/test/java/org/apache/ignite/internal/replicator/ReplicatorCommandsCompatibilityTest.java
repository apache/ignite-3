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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesSerializationRegistryInitializer;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of replicator raft commands. It is verified that deserialization of commands that
 * were created on earlier versions of the product will be error-free.
 */
public class ReplicatorCommandsCompatibilityTest extends BaseCommandsCompatibilityTest {
    private final ReplicaMessagesFactory factory = new ReplicaMessagesFactory();

    @Override
    protected Collection<MessageSerializationRegistryInitializer> initializers() {
        return List.of(new ReplicaMessagesSerializationRegistryInitializer());
    }

    @Override
    protected Collection<Command> commandsToSerialize() {
        return List.of(
                createSafeTimeSyncCommand(),
                createPrimaryReplicaChangeCommand()
        );
    }

    @Test
    @TestForCommand(SafeTimeSyncCommand.class)
    void testSafeTimeSyncCommand() {
        SafeTimeSyncCommand command = decodeCommand("CSlH");

        assertEquals(initiatorTime(), command.initiatorTime());
    }

    @Test
    @TestForCommand(PrimaryReplicaChangeCommand.class)
    void testPrimaryReplicaChangeCommand() {
        PrimaryReplicaChangeCommand command = decodeCommand("CSorAAAAAAAAAAAqAAAAAAAAAEUGbm9kZTE=");

        assertEquals(42, command.leaseStartTime());
        assertEquals(uuid(), command.primaryReplicaNodeId());
        assertEquals("node1", command.primaryReplicaNodeName());
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
}
