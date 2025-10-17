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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest;
import org.apache.ignite.internal.raft.Command;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of tx raft commands. It is verified that deserialization of commands that were
 * created on earlier versions of the product will be error-free.
 */
public class TxCommandsCompatibilityTest extends BaseCommandsCompatibilityTest {
    private final TxMessagesFactory factory = new TxMessagesFactory();

    @Override
    protected Collection<MessageSerializationRegistryInitializer> initializers() {
        return List.of(new TxMessagesSerializationRegistryInitializer());
    }

    @Override
    protected Collection<Command> commandsToSerialize() {
        return List.of(createVacuumTxStatesCommand());
    }

    @Test
    @TestForCommand(VacuumTxStatesCommand.class)
    void testVacuumTxStatesCommand() {
        VacuumTxStatesCommand command = decodeCommand("Bg4CAAAAAAAAAAAqAAAAAAAAAEU=");

        assertEquals(Set.of(uuid()), command.txIds());
    }

    private VacuumTxStatesCommand createVacuumTxStatesCommand() {
        return factory.vacuumTxStatesCommand()
                .txIds(Set.of(uuid()))
                .build();
    }
}
