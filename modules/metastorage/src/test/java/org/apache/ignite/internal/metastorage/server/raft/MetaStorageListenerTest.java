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

package org.apache.ignite.internal.metastorage.server.raft;

import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessagesFactory;
import org.apache.ignite.internal.metastorage.dsl.Statements;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class MetaStorageListenerTest {
    private static final MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();
    private static final MetaStorageMessagesFactory messagesFactory = new MetaStorageMessagesFactory();

    private static final long COMMAND_TERM = 3;

    private static final long PRE_TARGET_COMMAND_INDEX = 1;
    private static final long TARGET_COMMAND_INDEX = 2;

    private static final HybridClock clock = new HybridClockImpl();

    private KeyValueStorage storage;
    private ClusterTimeImpl clusterTime;

    private MetaStorageListener listener;

    @BeforeEach
    void setUp() {
        storage = new SimpleInMemoryKeyValueStorage("test");
        clusterTime = new ClusterTimeImpl("test", new IgniteSpinBusyLock(), clock);

        listener = new MetaStorageListener(storage, clock, clusterTime);

        storage.start();
    }

    @AfterEach
    void cleanup() throws Exception {
        if (listener != null) {
            listener.onShutdown();
        }

        closeAllManually(clusterTime, storage);
    }

    @ParameterizedTest
    @MethodSource("commandsVariationsForIndexAdvanceTesting")
    void commandsAdvanceLastAppliedIndex(MetaStorageWriteCommand command) {
        testCommandAdvancedLastAppliedIndex(command);
    }

    private static List<MetaStorageWriteCommand> commandsVariationsForIndexAdvanceTesting() {
        return List.of(
                somePutCommand(),
                commandsFactory.putAllCommand()
                        .keys(List.of(ByteBuffer.allocate(3)))
                        .values(List.of(ByteBuffer.allocate(3)))
                        .initiatorTime(clock.now())
                        .safeTime(clock.now())
                        .build(),
                commandsFactory.removeCommand()
                        .key(ByteBuffer.allocate(3))
                        .initiatorTime(clock.now())
                        .safeTime(clock.now())
                        .build(),
                commandsFactory.removeAllCommand()
                        .keys(List.of(ByteBuffer.allocate(3)))
                        .initiatorTime(clock.now())
                        .safeTime(clock.now())
                        .build(),
                commandsFactory.removeByPrefixCommand()
                        .prefix(ByteBuffer.allocate(3))
                        .initiatorTime(clock.now())
                        .safeTime(clock.now())
                        .build(),
                someInvokeCommand(),
                someMultiInvokeCommand(),

                // Normal command that gets applied.
                commandsFactory.syncTimeCommand()
                        .initiatorTime(clock.now())
                        .safeTime(clock.now())
                        .initiatorTerm(COMMAND_TERM)
                        .build(),

                // Command that gets discarded.
                commandsFactory.syncTimeCommand()
                        .initiatorTime(clock.now())
                        .safeTime(clock.now())
                        .initiatorTerm(COMMAND_TERM - 1)
                        .build()
        );
    }

    private void testCommandAdvancedLastAppliedIndex(MetaStorageWriteCommand command) {
        applySingleCommand(command, TARGET_COMMAND_INDEX, COMMAND_TERM);

        assertTargetIndexAndTermInStorage();
    }

    private void assertTargetIndexAndTermInStorage() {
        IndexWithTerm indexWithTerm = storage.getIndexWithTerm();

        assertThat(indexWithTerm, is(notNullValue()));
        assertThat(indexWithTerm.index(), is(TARGET_COMMAND_INDEX));
        assertThat(indexWithTerm.term(), is(COMMAND_TERM));
    }

    private void applySingleCommand(MetaStorageWriteCommand command, long index, long term) {
        Command updatedCommand = listener.onBeforeApply(command);
        listener.onWrite(List.of(commandClosure((MetaStorageWriteCommand) updatedCommand, index, term)).iterator());
    }

    private static CommandClosure<WriteCommand> commandClosure(MetaStorageWriteCommand command, long index, long term) {
        return new SimpleCommandClosure(command, index, term);
    }

    private static PutCommand somePutCommand() {
        return commandsFactory.putCommand()
                .key(ByteBuffer.allocate(3))
                .value(ByteBuffer.allocate(3))
                .initiatorTime(clock.now())
                .safeTime(clock.now())
                .build();
    }

    private static InvokeCommand someInvokeCommand() {
        return commandsFactory.invokeCommand()
                .id(messagesFactory.commandId().nodeId(UUID.randomUUID()).build())
                .condition(notExists(new ByteArray(new byte[3])))
                .success(List.of(noop()))
                .failure(List.of(noop()))
                .initiatorTime(clock.now())
                .safeTime(clock.now())
                .build();
    }

    private static MultiInvokeCommand someMultiInvokeCommand() {
        return commandsFactory.multiInvokeCommand()
                .id(messagesFactory.commandId().nodeId(UUID.randomUUID()).build())
                .iif(Statements.iif(
                        notExists(new ByteArray(new byte[3])),
                        ops(noop()).yield(true),
                        ops(noop()).yield(false)
                ))
                .initiatorTime(clock.now())
                .safeTime(clock.now())
                .build();
    }

    @Test
    void compactionCommandAdvancesLastAppliedIndex() {
        // We need to have at least one revision, otherwise the following compaction command will fail.
        createSomeRevision();

        MetaStorageWriteCommand command = commandsFactory.compactionCommand()
                .compactionRevision(0)
                .initiatorTime(clock.now())
                .safeTime(clock.now())
                .build();

        testCommandAdvancedLastAppliedIndex(command);
    }

    private void createSomeRevision() {
        applySingleCommand(somePutCommand(), PRE_TARGET_COMMAND_INDEX, COMMAND_TERM);
    }

    @Test
    void noOpEvictionCommandAdvancesLastAppliedIndex() {
        // This command will not evict anything.
        MetaStorageWriteCommand command = commandsFactory.evictIdempotentCommandsCacheCommand()
                .evictionTimestamp(HybridTimestamp.MIN_VALUE)
                .initiatorTime(clock.now())
                .safeTime(clock.now())
                .build();

        testCommandAdvancedLastAppliedIndex(command);
    }

    @Test
    void effectiveEvictionCommandAdvancesLastAppliedIndex() {
        applySingleCommand(someInvokeCommand(), PRE_TARGET_COMMAND_INDEX, COMMAND_TERM);

        // This command will evict something.
        MetaStorageWriteCommand command = commandsFactory.evictIdempotentCommandsCacheCommand()
                .evictionTimestamp(HybridTimestamp.MAX_VALUE)
                .initiatorTime(clock.now())
                .safeTime(clock.now())
                .build();

        testCommandAdvancedLastAppliedIndex(command);
    }

    @Test
    void noOpInvokeCommandAdvancesLastAppliedIndex() {
        MetaStorageWriteCommand command = someInvokeCommand();
        applySingleCommand(command, PRE_TARGET_COMMAND_INDEX, COMMAND_TERM);

        testCommandAdvancedLastAppliedIndex(command);
    }

    @Test
    void noOpMultiInvokeCommandAdvancesLastAppliedIndex() {
        MetaStorageWriteCommand command = someMultiInvokeCommand();
        applySingleCommand(command, PRE_TARGET_COMMAND_INDEX, COMMAND_TERM);

        testCommandAdvancedLastAppliedIndex(command);
    }

    @Test
    void applicationOfConfigurationAdvancesLastAppliedIndex() {
        listener.onConfigurationCommitted(
                new RaftGroupConfiguration(TARGET_COMMAND_INDEX, COMMAND_TERM, 1, 0, List.of("peer"), List.of(), null, null),
                TARGET_COMMAND_INDEX,
                COMMAND_TERM
        );

        assertTargetIndexAndTermInStorage();
    }

    private static class SimpleCommandClosure implements CommandClosure<WriteCommand> {
        private final MetaStorageWriteCommand command;
        private final long index;
        private final long term;

        private SimpleCommandClosure(MetaStorageWriteCommand command, long index, long term) {
            this.command = command;
            this.index = index;
            this.term = term;
        }

        @Override
        public WriteCommand command() {
            return command;
        }

        @Override
        public long index() {
            return index;
        }

        @Override
        public long term() {
            return term;
        }

        @Override
        public @Nullable HybridTimestamp safeTimestamp() {
            return command.safeTime();
        }

        @Override
        public void result(@Nullable Serializable res) {
            // No-op.
        }
    }
}
