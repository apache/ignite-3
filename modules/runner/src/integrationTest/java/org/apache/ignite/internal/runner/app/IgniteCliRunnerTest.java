/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.runner.app;

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager.REST_ENDPOINT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpHeaderNames;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.app.IgniteCliRunner;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests the start ignite nodes.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class IgniteCliRunnerTest {
    private final HttpClient httpClient = HttpClient.newHttpClient();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @WorkDirectory
    private Path workDir;

    /** TODO: Replace this test by full integration test on the cli side IGNITE-15097. */
    @Test
    public void smokeTestArgs() throws Exception {
        Path configPath = Path.of(IgniteCliRunnerTest.class.getResource("/ignite-config.json").toURI());

        CompletableFuture<Ignite> ign = supplyAsync(() -> IgniteCliRunner.start(
                new String[]{
                        "--config", configPath.toAbsolutePath().toString(),
                        "--work-dir", workDir.resolve("node").toAbsolutePath().toString(),
                        "node"
                }
        ));

        var nodeAddr = new NetworkAddress("localhost", 10300);

        init(nodeAddr, "node");

        assertThat(ign, willBe(notNullValue(Ignite.class)));

        ign.join().close();
    }

    @Test
    public void smokeTestArgsNullConfig() throws Exception {
        CompletableFuture<Ignite> ign = supplyAsync(() -> IgniteCliRunner.start(
                new String[]{
                        "--work-dir", workDir.resolve("node").toAbsolutePath().toString(),
                        "node"
                }
        ));

        var nodeAddr = new NetworkAddress("localhost", 10300);

        init(nodeAddr, "node");

        assertThat(ign, willBe(notNullValue(Ignite.class)));

        ign.join().close();
    }

    private void init(NetworkAddress addr, String metaStorageNodeName) throws Exception {
        var body = Map.of("metaStorageNodes", List.of(metaStorageNodeName));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URL("http", addr.host(), addr.port(), REST_ENDPOINT).toURI())
                .header(HttpHeaderNames.CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                .POST(BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(body)))
                .build();

        assertTrue(waitForCondition(() -> {
            try {
                HttpResponse<Void> response = httpClient.send(request, BodyHandlers.discarding());

                return response.statusCode() == 200;
            } catch (ConnectException ignored) {
                return false;
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 15000));
    }
}
