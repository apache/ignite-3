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

package org.apache.ignite.internal.partition.replicator.network.command;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.tx.message.EnlistedPartitionGroupMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxMessagesSerializationRegistryInitializer;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of partition raft commands. It is verified that deserialization of commands that
 * were created on earlier versions of the product will be error-free.
 */
public class PartitionCommandsCompatibilityTest extends BaseCommandsCompatibilityTest {
    private final PartitionReplicationMessagesFactory commandFactory = new PartitionReplicationMessagesFactory();

    private final ReplicaMessagesFactory replicaFactory = new ReplicaMessagesFactory();

    private final TxMessagesFactory txFactory = new TxMessagesFactory();

    @Override
    protected Collection<MessageSerializationRegistryInitializer> initializers() {
        return List.of(
                new PartitionReplicationMessagesSerializationRegistryInitializer(),
                new ReplicaMessagesSerializationRegistryInitializer(),
                new TxMessagesSerializationRegistryInitializer()
        );
    }

    @Override
    protected Collection<Command> commandsToSerialize() {
        return List.of(
                createBuildIndexCommand(),
                createBuildIndexCommandV2(),
                createBuildIndexCommandV3(),
                createFinishTxCommandV1(),
                createFinishTxCommandV2(),
                createUpdateAllCommand(),
                createUpdateAllCommandV2(),
                createUpdateCommand(),
                createUpdateCommandV2(),
                createUpdateMinimumActiveTxBeginTimeCommand(),
                createWriteIntentSwitchCommand(),
                createWriteIntentSwitchCommandV2()
        );
    }

    @Test
    @TestForCommand(BuildIndexCommand.class)
    void testBuildIndexCommand() {
        BuildIndexCommand command = decodeCommand("Ci0BRgIAAAAAAAAAACoAAAAAAAAARQ==");

        assertEquals(69, command.indexId());
        assertEquals(List.of(uuid()), command.rowIds());
        assertTrue(command.finish());
    }

    @Test
    @TestForCommand(BuildIndexCommandV2.class)
    void testBuildIndexCommandV2() {
        BuildIndexCommandV2 command = decodeCommand("CjIBRgIAAAAAAAAAACoAAAAAAAAARQg=");

        assertEquals(69, command.indexId());
        assertEquals(List.of(uuid()), command.rowIds());
        assertTrue(command.finish());
        assertEquals(7, command.tableId());
    }

    @Test
    @TestForCommand(BuildIndexCommandV3.class)
    void testBuildIndexCommandV3() {
        BuildIndexCommandV3 command = decodeCommand("CjQDAP/////////W/////////7sAAAAAAAAAACoAAAAAAAAARQFGAgAAAAAAAAAAKgAAAAAAAABFCA==");

        assertEquals(69, command.indexId());
        assertEquals(List.of(uuid()), command.rowIds());
        assertTrue(command.finish());
        assertEquals(7, command.tableId());
        assertThat(command.abortedTransactionIds(), containsInAnyOrder(uuid(), anotherUuid()));
    }

    @Test
    @TestForCommand(FinishTxCommandV1.class)
    void testFinishTxCommandV1() {
        FinishTxCommandV1 command = decodeCommand("CikBSAFHAgkrLSJGAAAAAAAAAAAqAAAAAAAAAEU=");

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.commit());
        assertTrue(command.full());
        assertEquals(commitTimestamp(), command.commitTimestamp());
        assertEquals(List.of(tablePartitionId()), command.partitionIds());
    }

    @Test
    @TestForCommand(FinishTxCommandV2.class)
    void testFinishTxCommandV2() {
        FinishTxCommandV2 command = decodeCommand("CjMBSAFHAgYVCSwXDAMtIkYAAAAAAAAAACoAAAAAAAAARQ==");

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.full());
        assertTrue(command.commit());
        assertEquals(commitTimestamp(), command.commitTimestamp());
        assertEquals(List.of(enlistedPartitionGroup()), command.partitions());
    }

    @Test
    @TestForCommand(UpdateAllCommand.class)
    void testUpdateAllCommand() {
        UpdateAllCommand command = decodeCommand(
                "CisBRwErAgAAAAAAAAAAKgAAAAAAAABFChkKEwMEAQIDAdMJRgkrLSIAAAAAAAAAACoAAAAAAAAARQAAAAAAAAAAKgAAAAAAAABF"
        );

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.full());
        assertEquals(tablePartitionId(), command.commitPartitionId());
        assertEquals(Map.of(uuid(), timedBinaryRowMessage()), command.messageRowsToUpdate());
        assertEquals(uuid(), command.txCoordinatorId());
        assertEquals(42L, command.leaseStartTime());
    }

    @Test
    @TestForCommand(UpdateAllCommandV2.class)
    void testUpdateAllCommandV2() {
        UpdateAllCommandV2 command = decodeCommand(
                "CjEBRwErAgAAAAAAAAAAKgAAAAAAAABFChkKEwMEAQIDAdMJRggJKy0iAAAAAAAAAAAqAAAAAAAAAEUAAAAAAAAAACoAAAAAAAAARQ=="
        );

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.full());
        assertEquals(tablePartitionId(), command.commitPartitionId());
        assertEquals(Map.of(uuid(), timedBinaryRowMessage()), command.messageRowsToUpdate());
        assertEquals(uuid(), command.txCoordinatorId());
        assertEquals(42L, command.leaseStartTime());
        assertEquals(7, command.tableId());
    }

    @Test
    @TestForCommand(UpdateCommand.class)
    void testUpdateCommand() {
        UpdateCommand command = decodeCommand(
                "CiwBRwErChkKEwMEAQIDAdMJAAAAAAAAAAAqAAAAAAAAAEVGCSstIgAAAAAAAAAAKgAAAAAAAABFAAAAAAAAAAAqAAAAAAAAAEU="
        );

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.full());
        assertEquals(tablePartitionId(), command.commitPartitionId());
        assertEquals(uuid(), command.rowUuid());
        assertEquals(timedBinaryRowMessage(), command.messageRowToUpdate());
        assertEquals(uuid(), command.txCoordinatorId());
        assertEquals(42L, command.leaseStartTime());
    }

    @Test
    @TestForCommand(UpdateCommandV2.class)
    void testUpdateCommandV2() {
        UpdateCommandV2 command = decodeCommand(
                "CjABRwErChkKEwMEAQIDAdMJAAAAAAAAAAAqAAAAAAAAAEVGCAkrLSIAAAAAAAAAACoAAAAAAAAARQAAAAAAAAAAKgAAAAAAAABF"
        );

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.full());
        assertEquals(tablePartitionId(), command.commitPartitionId());
        assertEquals(uuid(), command.rowUuid());
        assertEquals(timedBinaryRowMessage(), command.messageRowToUpdate());
        assertEquals(uuid(), command.txCoordinatorId());
        assertEquals(42L, command.leaseStartTime());
        assertEquals(7, command.tableId());
    }

    @Test
    @TestForCommand(UpdateMinimumActiveTxBeginTimeCommand.class)
    void testUpdateMinimumActiveTxBeginTimeCommand() {
        UpdateMinimumActiveTxBeginTimeCommand command = decodeCommand("Ci5HRtMJ");

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(1234L, command.timestamp());
    }

    @Test
    @TestForCommand(WriteIntentSwitchCommand.class)
    void testWriteIntentSwitchCommand() {
        WriteIntentSwitchCommand command = decodeCommand("CioBSAFHRgAAAAAAAAAAKgAAAAAAAABF");

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.full());
        assertTrue(command.commit());
        assertEquals(commitTimestamp(), command.commitTimestamp());
    }

    @Test
    @TestForCommand(WriteIntentSwitchCommandV2.class)
    void testWriteIntentSwitchCommandV2() {
        WriteIntentSwitchCommandV2 command = decodeCommand("Ci8BSAFHRgMJCAAAAAAAAAAAKgAAAAAAAABF");

        assertEquals(initiatorTime(), command.initiatorTime());
        assertEquals(safeTime(), command.safeTime());
        assertEquals(uuid(), command.txId());
        assertTrue(command.full());
        assertTrue(command.commit());
        assertEquals(commitTimestamp(), command.commitTimestamp());
        assertEquals(Set.of(7, 8), command.tableIds());
    }

    private TablePartitionIdMessage tablePartitionId() {
        return replicaFactory.tablePartitionIdMessage()
                .tableId(33)
                .partitionId(44)
                .build();
    }

    private ZonePartitionIdMessage zonePartitionId() {
        return replicaFactory.zonePartitionIdMessage()
                .zoneId(11)
                .partitionId(22)
                .build();
    }

    private EnlistedPartitionGroupMessage enlistedPartitionGroup() {
        return txFactory.enlistedPartitionGroupMessage()
                .groupId(zonePartitionId())
                .tableIds(Set.of(33, 44))
                .build();
    }

    private TimedBinaryRowMessage timedBinaryRowMessage() {
        return commandFactory.timedBinaryRowMessage()
                .binaryRowMessage(binaryRow())
                .timestamp(HybridTimestamp.hybridTimestamp(1234))
                .build();
    }

    private BinaryRowMessage binaryRow() {
        return commandFactory.binaryRowMessage()
                .binaryTuple(ByteBuffer.wrap(new byte[]{1, 2, 3}))
                .build();
    }

    private WriteIntentSwitchCommandV2 createWriteIntentSwitchCommandV2() {
        return commandFactory.writeIntentSwitchCommandV2()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commit(true)
                .commitTimestamp(commitTimestamp())
                .tableIds(Set.of(7, 8))
                .build();
    }

    private WriteIntentSwitchCommand createWriteIntentSwitchCommand() {
        return commandFactory.writeIntentSwitchCommand()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commit(true)
                .commitTimestamp(commitTimestamp())
                .build();
    }

    private UpdateMinimumActiveTxBeginTimeCommand createUpdateMinimumActiveTxBeginTimeCommand() {
        return commandFactory.updateMinimumActiveTxBeginTimeCommand()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .timestamp(1234L)
                .build();
    }

    private UpdateCommandV2 createUpdateCommandV2() {
        return commandFactory.updateCommandV2()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commitPartitionId(tablePartitionId())
                .rowUuid(uuid())
                .messageRowToUpdate(timedBinaryRowMessage())
                .txCoordinatorId(uuid())
                .leaseStartTime(42L)
                .tableId(7)
                .build();
    }

    private UpdateCommand createUpdateCommand() {
        return commandFactory.updateCommand()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commitPartitionId(tablePartitionId())
                .rowUuid(uuid())
                .messageRowToUpdate(timedBinaryRowMessage())
                .txCoordinatorId(uuid())
                .leaseStartTime(42L)
                .build();
    }

    private UpdateAllCommandV2 createUpdateAllCommandV2() {
        return commandFactory.updateAllCommandV2()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commitPartitionId(tablePartitionId())
                .messageRowsToUpdate(Map.of(uuid(), timedBinaryRowMessage()))
                .txCoordinatorId(uuid())
                .leaseStartTime(42L)
                .tableId(7)
                .build();
    }

    private UpdateAllCommand createUpdateAllCommand() {
        return commandFactory.updateAllCommand()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commitPartitionId(tablePartitionId())
                .messageRowsToUpdate(Map.of(uuid(), timedBinaryRowMessage()))
                .txCoordinatorId(uuid())
                .leaseStartTime(42L)
                .build();
    }

    private FinishTxCommandV2 createFinishTxCommandV2() {
        return commandFactory.finishTxCommandV2()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commit(true)
                .commitTimestamp(commitTimestamp())
                .partitions(List.of(enlistedPartitionGroup()))
                .build();
    }

    private FinishTxCommandV1 createFinishTxCommandV1() {
        return commandFactory.finishTxCommandV1()
                .initiatorTime(initiatorTime())
                .safeTime(safeTime())
                .txId(uuid())
                .full(true)
                .commit(true)
                .commitTimestamp(commitTimestamp())
                .partitionIds(List.of(tablePartitionId()))
                .build();
    }

    private BuildIndexCommandV2 createBuildIndexCommandV2() {
        return commandFactory.buildIndexCommandV2()
                .indexId(69)
                .rowIds(List.of(uuid()))
                .finish(true)
                .tableId(7)
                .build();
    }

    private BuildIndexCommandV2 createBuildIndexCommandV3() {
        return commandFactory.buildIndexCommandV3()
                .indexId(69)
                .rowIds(List.of(uuid()))
                .finish(true)
                .tableId(7)
                .abortedTransactionIds(Set.of(uuid(), anotherUuid()))
                .build();
    }

    private BuildIndexCommand createBuildIndexCommand() {
        return commandFactory.buildIndexCommand()
                .indexId(69)
                .rowIds(List.of(uuid()))
                .finish(true)
                .build();
    }
}
