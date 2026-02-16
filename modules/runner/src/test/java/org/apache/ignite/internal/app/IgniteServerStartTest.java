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

package org.apache.ignite.internal.app;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.DEFAULT_CONFIG_NAME;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.writeConfigurationFile;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToObject;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.NodeStartException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for {@link IgniteServer} starting and errors handling during the process.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class IgniteServerStartTest extends BaseIgniteAbstractTest {
    private static final int IGNITE_SERVER_PORT = 3344;

    private static final String IGNITE_SERVER_NAME = "test-node-" + IGNITE_SERVER_PORT;

    private static final String IGNITE_SERVER_CONFIGURATION = "ignite {\n"
            + "  network: {\n"
            + "    port: " + IGNITE_SERVER_PORT + ",\n"
            + "    nodeFinder.netClusterNodes: [ \"localhost:" + IGNITE_SERVER_PORT + "\" ]\n"
            + "  },\n"
            + "  clientConnector.port: 10800,\n"
            + "  rest.port: 10300,\n"
            + "  failureHandler.dumpThreadsOnFailure: false,\n"
            + "  storage.profiles.default {engine: aipersist, sizeBytes: " + 256 * MiB + "}\n"
            + "}";

    @WorkDirectory
    private static Path workDir;

    private IgniteServer server;

    @BeforeEach
    public void createIgniteServerMock() throws IOException {
        server = spy(createIgniteServer());
    }

    private static IgniteServer createIgniteServer() throws IOException {
        Files.createDirectories(workDir);
        Path configPath = workDir.resolve(DEFAULT_CONFIG_NAME);

        writeConfigurationFile(IGNITE_SERVER_CONFIGURATION, configPath);

        return IgniteServer
                .builder(IGNITE_SERVER_NAME, configPath, workDir)
                .build();
    }

    @AfterEach
    public void shutdownIgniteServerMock() {
        if (server == null) {
            return;
        }

        reset(server);

        server.shutdown();
    }

    @Test
    void igniteServerStartTest() {
        Assertions.assertDoesNotThrow(() -> server.start());
    }

    @Test
    void errorDuringIgniteServerStartTest() {
        Error error = new Error("Test error.");

        when(server.startAsync()).thenReturn(failedFuture(error));

        NodeStartException exception = assertThrows(
                NodeStartException.class,
                server::start,
                "Error occurred during node start, check .jar libraries and JVM execution arguments."
        );

        Throwable cause = exception.getCause();
        assertThat(cause, is(notNullValue()));
        assertThat(cause, is(equalToObject(error)));
    }

    @Test
    void initializerErrorWithoutCauseDuringIgniteServerStartTest() {
        ExceptionInInitializerError initializerError = new ExceptionInInitializerError("Test initializer error.");

        when(server.startAsync()).thenReturn(failedFuture(initializerError));

        NodeStartException exception = assertThrows(
                NodeStartException.class,
                server::start,
                "Error during static components initialization with unknown cause, check .jar libraries and JVM execution arguments."
        );

        Throwable cause = exception.getCause();
        assertThat(cause, is(notNullValue()));
        assertThat(cause, is(equalToObject(initializerError)));
    }

    @Test
    void initializerErrorWithIllegalAccessCauseDuringIgniteServerStartTest() {
        IllegalAccessException illegalAccessCause = new IllegalAccessException("Test illegal access exception.");
        ExceptionInInitializerError initializerError = new ExceptionInInitializerError(illegalAccessCause);

        when(server.startAsync()).thenReturn(failedFuture(initializerError));

        NodeStartException exception = assertThrows(
                NodeStartException.class,
                server::start,
                "Error during static components initialization due to illegal code access, check --add-opens JVM execution arguments."
        );

        Throwable cause = exception.getCause();
        assertThat(cause, is(notNullValue()));
        assertThat(cause, is(equalToObject(illegalAccessCause)));
    }

    @Test
    void initializerErrorWithCommonCauseDuringIgniteServerStartTest() {
        Throwable commonCause = new Throwable("Test common exception.");
        ExceptionInInitializerError initializerError = new ExceptionInInitializerError(commonCause);

        when(server.startAsync()).thenReturn(failedFuture(initializerError));

        NodeStartException exception = assertThrows(
                NodeStartException.class,
                server::start,
                "Error during static components initialization, check .jar libraries and JVM execution arguments."
        );

        Throwable cause = exception.getCause();
        assertThat(cause, is(notNullValue()));
        assertThat(cause, is(equalToObject(commonCause)));
    }
}
