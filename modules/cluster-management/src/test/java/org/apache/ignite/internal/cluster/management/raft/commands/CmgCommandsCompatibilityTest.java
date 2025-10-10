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

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of CMG raft commands. It is verified that deserialization of commands that were
 * created on earlier versions of the product will be error-free.
 *
 * <p>For MAC users with aarch64 architecture, you will need to add {@code || "aarch64".equals(arch)} to the
 * {@code GridUnsafe#unaligned()} for the tests to pass. For more details, see
 * <a href="https://lists.apache.org/thread/67coyvm8mo7106mkndt24yqwtbvb7590">discussion</a>.</p>
 *
 * <p>To serialize commands, use {@link #serializeAll()} and insert the result into the appropriate tests.</p>
 */
public class CmgCommandsCompatibilityTest extends BaseIgniteAbstractTest {
    private final MessageSerializationRegistry registry = new MessageSerializationRegistryImpl();

    private final Marshaller marshaller = new ThreadLocalOptimizedMarshaller(registry);

    private final CmgMessagesFactory factory = new CmgMessagesFactory();

    @BeforeEach
    void setUp() {
        new CmgMessagesSerializationRegistryInitializer().registerFactories(registry);
    }

    @Test
    void testChangeMetaStorageInfoCommand() {
        ChangeMetaStorageInfoCommand command = decodeCommand("CDADCG1zTm9kZTIIbXNOb2RlMQEr");

        assertEquals(Set.of("msNode1", "msNode2"), command.metaStorageNodes());
        assertEquals(42, command.metastorageRepairingConfigIndex());
    }

    @Test
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
    void testJoinReadyCommand() {
        JoinReadyCommand command = decodeCommand(
                "CC0IPQZob3N0MQAAAAAAAAAAKgAAAAAAAABFBm5hbWUx6QcCCXByb2ZpbGUxAghzeXNLZXkxCHN5c1ZhbDECCXVzZXJLZXkxCXVzZXJWYWwx"
        );

        assertEquals(createClusterNodeMessage(), command.node());
    }

    @Test
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
    void testNodesLeaveCommand() {
        NodesLeaveCommand command = decodeCommand(
                "CC4CCD0GaG9zdDEAAAAAAAAAACoAAAAAAAAARQZuYW1lMekHAglwcm9maWxlMQIIc3lzS2V5MQhzeXNWYWwxAgl1c2VyS2V5MQl1c2VyVmFsMQ=="
        );

        assertEquals(Set.of(createClusterNodeMessage()), command.nodes());
    }

    @Test
    void testReadLogicalTopologyCommand() {
        Command command = decodeCommand("CCs=");

        assertInstanceOf(ReadLogicalTopologyCommand.class, command);
    }

    @Test
    void testReadMetaStorageInfoCommand() {
        Command command = decodeCommand("CEM=");

        assertInstanceOf(ReadMetaStorageInfoCommand.class, command);
    }

    @Test
    void testReadStateCommand() {
        Command command = decodeCommand("CCo=");

        assertInstanceOf(ReadStateCommand.class, command);
    }

    @Test
    void testReadValidatedNodesCommand() {
        Command command = decodeCommand("CC8=");

        assertInstanceOf(ReadValidatedNodesCommand.class, command);
    }

    private static UUID uuid() {
        return new UUID(42, 69);
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

    private <T extends Command> T deserializeCommand(byte[] bytes) {
        return marshaller.unmarshall(bytes);
    }

    private <T extends Command> T decodeCommand(String base64) {
        return deserializeCommand(Base64.getDecoder().decode(base64));
    }

    @SuppressWarnings("unused")
    private void serializeAll() {
        List<Command> commands = List.of(
                createChangeMetaStorageInfoCommand(),
                createInitCmgStateCommand(),
                createJoinReadyCommand(),
                createJoinRequestCommand(),
                createNodesLeaveCommand(),
                createReadLogicalTopologyCommand(),
                createReadMetaStorageInfoCommand(),
                createReadStateCommand(),
                createReadValidatedNodesCommand()
        );

        for (Command c : commands) {
            log.info(">>>>> Serialized command: [c={}, base64='{}']", c.getClass().getSimpleName(), encodeCommand(c));
        }
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

    private ChangeMetaStorageInfoCommand createChangeMetaStorageInfoCommand() {
        return factory.changeMetaStorageInfoCommand()
                .metaStorageNodes(Set.of("msNode1", "msNode2"))
                .metastorageRepairingConfigIndex(42L)
                .build();
    }

    private byte[] serializeCommand(Command c) {
        return marshaller.marshall(c);
    }

    private String encodeCommand(Command c) {
        return Base64.getEncoder().encodeToString(serializeCommand(c));
    }
}
