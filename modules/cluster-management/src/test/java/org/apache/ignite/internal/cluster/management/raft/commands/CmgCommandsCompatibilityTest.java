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

package org.apache.ignite.internal.cluster.management.raft.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest;
import org.apache.ignite.internal.raft.Command;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of CMG raft commands. It is verified that deserialization of commands that were
 * created on earlier versions of the product will be error-free.
 */
public class CmgCommandsCompatibilityTest extends BaseCommandsCompatibilityTest {
    private final CmgMessagesFactory factory = new CmgMessagesFactory();

    @Override
    protected Collection<MessageSerializationRegistryInitializer> initializers() {
        return List.of(new CmgMessagesSerializationRegistryInitializer());
    }

    @Override
    protected Collection<Command> commandsToSerialize() {
        return List.of(
                createChangeMetaStorageInfoCommand(),
                createChangeClusterNameCommand(),
                createInitCmgStateCommand(),
                createJoinReadyCommand(),
                createJoinRequestCommand(),
                createNodesLeaveCommand(),
                createReadLogicalTopologyCommand(),
                createReadMetaStorageInfoCommand(),
                createReadStateCommand(),
                createReadValidatedNodesCommand()
        );
    }

    @Test
    @TestForCommand(ChangeClusterNameCommand.class)
    void testChangeClusterNameCommand() {
        ChangeClusterNameCommand command = decodeCommand("CEANY2x1c3Rlck5hbWUy");

        assertEquals("clusterName2", command.clusterName());
    }

    @Test
    @TestForCommand(ChangeMetaStorageInfoCommand.class)
    void testChangeMetaStorageInfoCommand() {
        ChangeMetaStorageInfoCommand command = decodeCommand("CDADCG1zTm9kZTIIbXNOb2RlMQEr");

        assertEquals(Set.of("msNode1", "msNode2"), command.metaStorageNodes());
        assertEquals(42, command.metastorageRepairingConfigIndex());
    }

    @Test
    @TestForCommand(InitCmgStateCommand.class)
    void testInitCmgStateCommand() {
        InitCmgStateCommand command = decodeCommand(
                "CCkIPgg/AAAAAAAAAAAqAAAAAAAAAEUNY2x1c3Rlck5hbWUxAwljbWdOb2RlMQljbWdOb2RlMgIAAAAAAAAAACoAAAAAAAAARQxpbml0Q29uZmlnMQMIbXNOb"
                        + "2RlMQhtc05vZGUyCXZlcnNpb24xCD0GaG9zdDEAAAAAAAAAACoAAAAAAAAARQZuYW1lMekHAglwcm9maWxlMQIIc3lzS2V5MQhzeXNWYWwxAgl1"
                        + "c2VyS2V5MQl1c2VyVmFsMQ=="
        );

        assertEquals(createClusterNodeMessage(), command.node());
        assertEquals(createClusterState(), command.clusterState());
    }

    @Test
    @TestForCommand(JoinReadyCommand.class)
    void testJoinReadyCommand() {
        JoinReadyCommand command = decodeCommand(
                "CC0IPQZob3N0MQAAAAAAAAAAKgAAAAAAAABFBm5hbWUx6QcCCXByb2ZpbGUxAghzeXNLZXkxCHN5c1ZhbDECCXVzZXJLZXkxCXVzZXJWYWwx"
        );

        assertEquals(createClusterNodeMessage(), command.node());
    }

    @Test
    @TestForCommand(JoinRequestCommand.class)
    void testJoinRequestCommand() {
        JoinRequestCommand command = decodeCommand(
                "CCwIPwAAAAAAAAAAKgAAAAAAAABFDWNsdXN0ZXJOYW1lMQg9Bmhvc3QxAAAAAAAAAAAqAAAAAAAAAEUGbmFtZTHpBwIJcHJvZmlsZTECCHN5c0tleTEIc3lzV"
                        + "mFsMQIJdXNlcktleTEJdXNlclZhbDEJdmVyc2lvbjE="
        );

        assertEquals(createClusterNodeMessage(), command.node());
        assertEquals("version1", command.version());
        assertEquals(ClusterTag.clusterTag(factory, "clusterName1", uuid()), command.clusterTag());
    }

    @Test
    @TestForCommand(NodesLeaveCommand.class)
    void testNodesLeaveCommand() {
        NodesLeaveCommand command = decodeCommand(
                "CC4CCD0GaG9zdDEAAAAAAAAAACoAAAAAAAAARQZuYW1lMekHAglwcm9maWxlMQIIc3lzS2V5MQhzeXNWYWwxAgl1c2VyS2V5MQl1c2VyVmFsMQ=="
        );

        assertEquals(Set.of(createClusterNodeMessage()), command.nodes());
    }

    @Test
    @TestForCommand(ReadLogicalTopologyCommand.class)
    void testReadLogicalTopologyCommand() {
        Command command = decodeCommand("CCs=");

        assertInstanceOf(ReadLogicalTopologyCommand.class, command);
    }

    @Test
    @TestForCommand(ReadMetaStorageInfoCommand.class)
    void testReadMetaStorageInfoCommand() {
        Command command = decodeCommand("CEM=");

        assertInstanceOf(ReadMetaStorageInfoCommand.class, command);
    }

    @Test
    @TestForCommand(ReadStateCommand.class)
    void testReadStateCommand() {
        Command command = decodeCommand("CCo=");

        assertInstanceOf(ReadStateCommand.class, command);
    }

    @Test
    @TestForCommand(ReadValidatedNodesCommand.class)
    void testReadValidatedNodesCommand() {
        Command command = decodeCommand("CC8=");

        assertInstanceOf(ReadValidatedNodesCommand.class, command);
    }

    private ClusterNodeMessage createClusterNodeMessage() {
        return factory.clusterNodeMessage()
                .id(uuid())
                .name("name1")
                .host("host1")
                .port(1000)
                .userAttributes(Map.of("userKey1", "userVal1"))
                .systemAttributes(Map.of("sysKey1", "sysVal1"))
                .storageProfiles(List.of("profile1"))
                .build();
    }

    private ClusterState createClusterState() {
        return factory.clusterState()
                .cmgNodes(Set.of("cmgNode1", "cmgNode2"))
                .metaStorageNodes(Set.of("msNode1", "msNode2"))
                .version("version1")
                .clusterTag(ClusterTag.clusterTag(factory, "clusterName1", uuid()))
                .initialClusterConfiguration("initConfig1")
                .formerClusterIds(List.of(uuid()))
                .build();
    }

    private ReadValidatedNodesCommand createReadValidatedNodesCommand() {
        return factory.readValidatedNodesCommand().build();
    }

    private ReadStateCommand createReadStateCommand() {
        return factory.readStateCommand().build();
    }

    private ReadMetaStorageInfoCommand createReadMetaStorageInfoCommand() {
        return factory.readMetaStorageInfoCommand().build();
    }

    private ReadLogicalTopologyCommand createReadLogicalTopologyCommand() {
        return factory.readLogicalTopologyCommand().build();
    }

    private NodesLeaveCommand createNodesLeaveCommand() {
        return factory.nodesLeaveCommand()
                .nodes(Set.of(createClusterNodeMessage()))
                .build();
    }

    private JoinRequestCommand createJoinRequestCommand() {
        return factory.joinRequestCommand()
                .node(createClusterNodeMessage())
                .version("version1")
                .clusterTag(ClusterTag.clusterTag(factory, "clusterName1", uuid()))
                .build();
    }

    private JoinReadyCommand createJoinReadyCommand() {
        return factory.joinReadyCommand()
                .node(createClusterNodeMessage())
                .build();
    }

    private InitCmgStateCommand createInitCmgStateCommand() {
        return factory.initCmgStateCommand()
                .node(createClusterNodeMessage())
                .clusterState(createClusterState())
                .build();
    }

    private ChangeClusterNameCommand createChangeClusterNameCommand() {
        return factory.changeClusterNameCommand()
                .clusterName("clusterName2")
                .build();
    }

    private ChangeMetaStorageInfoCommand createChangeMetaStorageInfoCommand() {
        return factory.changeMetaStorageInfoCommand()
                .metaStorageNodes(Set.of("msNode1", "msNode2"))
                .metastorageRepairingConfigIndex(42L)
                .build();
    }
}
