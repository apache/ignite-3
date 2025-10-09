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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Compatibility testing for serialization/deserialization of CMG raft commands. It is verified that deserialization of commands that were
 * created on earlier versions of the product will be error-free.
 */
public class CmgCommandsCompatibilityTest extends BaseIgniteAbstractTest {
    private static final MessageSerializationRegistry REGISTRY = new MessageSerializationRegistryImpl();

    private static final Marshaller MARSHALLER = new ThreadLocalOptimizedMarshaller(REGISTRY);

    private static final CmgMessagesFactory FACTORY = new CmgMessagesFactory();

    @BeforeAll
    static void beforeAll() {
        new CmgMessagesSerializationRegistryInitializer().registerFactories(REGISTRY);
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
                "CCkIPgg/ACoAAAAAAAAARQAAAAAAAAANY2x1c3Rlck5hbWUxAwljbWdOb2RlMgljbWdOb2RlMQIAKgAAAAAAAABFAAAAAAAAAAxpbml0Q29uZmlnMQMIbXNOb"
                        + "2RlMghtc05vZGUxCXZlcnNpb24xCD0GaG9zdDEAKgAAAAAAAABFAAAAAAAAAAZuYW1lMekHAglwcm9maWxlMQIIc3lzS2V5MQhzeXNWYWwxAgl1"
                        + "c2VyS2V5MQl1c2VyVmFsMQ=="
        );

        assertEquals(createClusterNodeMessage(), command.node());
        assertEquals(createClusterState(), command.clusterState());
    }

    @Test
    void testJoinReadyCommand() {
        JoinReadyCommand command = decodeCommand(
                "CC0IPQZob3N0MQAqAAAAAAAAAEUAAAAAAAAABm5hbWUx6QcCCXByb2ZpbGUxAghzeXNLZXkxCHN5c1ZhbDECCXVzZXJLZXkxCXVzZXJWYWwx"
        );

        assertEquals(createClusterNodeMessage(), command.node());
    }

    @Test
    void testJoinRequestCommand() {
        JoinRequestCommand command = decodeCommand(
                "CCwIPwAqAAAAAAAAAEUAAAAAAAAADWNsdXN0ZXJOYW1lMQg9Bmhvc3QxACoAAAAAAAAARQAAAAAAAAAGbmFtZTHpBwIJcHJvZmlsZTECCHN5c0tleTEIc3lzV"
                        + "mFsMQIJdXNlcktleTEJdXNlclZhbDEJdmVyc2lvbjE="
        );

        assertEquals(createClusterNodeMessage(), command.node());
        assertEquals("version1", command.version());
        assertEquals(ClusterTag.clusterTag(FACTORY, "clusterName1", uuid()), command.clusterTag());
    }

    @Test
    void testNodesLeaveCommand() {
        NodesLeaveCommand command = decodeCommand(
                "CC4CCD0GaG9zdDEAKgAAAAAAAABFAAAAAAAAAAZuYW1lMekHAglwcm9maWxlMQIIc3lzS2V5MQhzeXNWYWwxAgl1c2VyS2V5MQl1c2VyVmFsMQ=="
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

    private static ClusterNodeMessage createClusterNodeMessage() {
        return FACTORY.clusterNodeMessage()
                .id(uuid())
                .name("name1")
                .host("host1")
                .port(1000)
                .userAttributes(Map.of("userKey1", "userVal1"))
                .systemAttributes(Map.of("sysKey1", "sysVal1"))
                .storageProfiles(List.of("profile1"))
                .build();
    }

    private static ClusterState createClusterState() {
        return FACTORY.clusterState()
                .cmgNodes(Set.of("cmgNode1", "cmgNode2"))
                .metaStorageNodes(Set.of("msNode1", "msNode2"))
                .version("version1")
                .clusterTag(ClusterTag.clusterTag(FACTORY, "clusterName1", uuid()))
                .initialClusterConfiguration("initConfig1")
                .formerClusterIds(List.of(uuid()))
                .build();
    }

    private static <T extends Command> T deserializeCommand(byte[] bytes) {
        return MARSHALLER.unmarshall(bytes);
    }

    private static <T extends Command> T decodeCommand(String base64) {
        return deserializeCommand(Base64.getDecoder().decode(base64));
    }
}
