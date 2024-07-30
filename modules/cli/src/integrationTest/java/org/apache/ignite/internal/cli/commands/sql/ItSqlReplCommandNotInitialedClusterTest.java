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

package org.apache.ignite.internal.cli.commands.sql;

import static org.junit.jupiter.api.Assertions.assertAll;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Replaces;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for {@link SqlReplCommand} with not initialized cluster. */
public class ItSqlReplCommandNotInitialedClusterTest extends CliIntegrationTest {
    private final Session session = new Session();

    @Bean
    @Replaces(ReplExecutorProvider.class)
    public ReplExecutorProvider replExecutorProvider() {
        return () -> repl -> {};
    }

    @Bean
    @Replaces(Session.class)
    public Session session() {
        return session;
    }

    @Override
    protected boolean needInitializeCluster() {
        return false;
    }

    @Test
    @DisplayName("Should throw error because cluster not initialized.")
    void nonExistingFile() {
        execute("sql", "--jdbc-url", JDBC_URL, "CREATE TABLE T(K INT PRIMARY KEY)");

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Connection refused: no further information:")
        );

        IgniteServer node0 = CLUSTER.startEmbeddedNode(0);
        IgniteServer node1 = CLUSTER.startEmbeddedNode(1);
        IgniteServer node2 = CLUSTER.startEmbeddedNode(2);

        session.onConnect(SessionInfo
                .builder()
                .jdbcUrl(JDBC_URL)
                .nodeUrl(NODE_URL)
                .build()
        );

        execute("sql", "--jdbc-url", JDBC_URL, "CREATE TABLE T(K INT PRIMARY KEY)");

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Probably, you have not initialized the cluster, try to run")
        );

        node0.initCluster(InitParameters.builder().clusterName("cluster")
                .cmgNodes(node0, node1, node2)
                .metaStorageNodes(node0, node1, node2).build());

        execute("sql", "--jdbc-url", JDBC_URL, "CREATE TABLE T(K INT PRIMARY KEY)");

        assertAll(
                this::assertOutputIsNotEmpty,
                this::assertErrOutputIsEmpty
        );
    }
}
