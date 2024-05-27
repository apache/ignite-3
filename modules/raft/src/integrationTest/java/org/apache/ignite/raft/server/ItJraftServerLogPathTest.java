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

package org.apache.ignite.raft.server;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.TestJraftServerFactory;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for checking that JRaftServer uses log storage path from configuration. */
class ItJraftServerLogPathTest extends RaftServerAbstractTest {
    private Path dataPath;
    private JraftServerImpl server;

    @BeforeEach
    void setUp() {
        dataPath = workDir.resolve("node0");
    }

    @AfterEach
    void tearDown() {
        assertThat(server.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    @WithSystemProperty(key = SharedLogStorageFactoryUtils.LOGIT_STORAGE_ENABLED_PROPERTY, value = "false")
    void testDefaultFactory() {
        Path logPath = workDir.resolve("db/log");
        assertThat(raftConfiguration.logPath().update(logPath.toString()), willCompleteSuccessfully());

        server = startServer(raftConfiguration);

        assertTrue(Files.exists(logPath));
    }

    @Test
    @WithSystemProperty(key = SharedLogStorageFactoryUtils.LOGIT_STORAGE_ENABLED_PROPERTY, value = "true")
    void testLogitFactory() {
        Path logPath = workDir.resolve("db/log");
        assertThat(raftConfiguration.logPath().update(logPath.toString()), willCompleteSuccessfully());

        server = startServer(raftConfiguration);

        LogitLogStorageFactory factory = (LogitLogStorageFactory) server.getLogStorageFactory();
        assertEquals(logPath.resolve("log-1"), factory.resolveLogStoragePath("1"));
    }

    @Test
    @WithSystemProperty(key = SharedLogStorageFactoryUtils.LOGIT_STORAGE_ENABLED_PROPERTY, value = "false")
    void testDefaultLogPathDefaultFactory() {
        server = startServer(raftConfiguration);

        assertTrue(Files.exists(dataPath.resolve("log")));
    }

    @Test
    @WithSystemProperty(key = SharedLogStorageFactoryUtils.LOGIT_STORAGE_ENABLED_PROPERTY, value = "true")
    void testDefaultLogPathLogitFactory() {
        server = startServer(raftConfiguration);

        LogitLogStorageFactory factory = (LogitLogStorageFactory) server.getLogStorageFactory();
        assertEquals(dataPath.resolve("log/log-1"), factory.resolveLogStoragePath("1"));
    }

    private JraftServerImpl startServer(RaftConfiguration raftConfiguration) {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService(PORT, List.of(addr), true);

        JraftServerImpl server = TestJraftServerFactory.create(
                service,
                dataPath,
                raftConfiguration,
                new NodeOptions(),
                new RaftGroupEventsClientListener()
        );

        assertThat(server.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return server;
    }
}
