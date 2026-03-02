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

package org.apache.ignite.internal.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for integration REST test.
 */
public abstract class AbstractRestTestBase extends ClusterPerTestIntegrationTest {
    /** HTTP host and port url part. */
    protected static final String HTTP_HOST_PORT = "http://localhost:10300";

    protected ObjectMapper objectMapper = new ObjectMapper();

    /** HTTP client. */
    private HttpClient client;

    protected static HttpRequest get(String path) {
        return get(HTTP_HOST_PORT, path);
    }

    public static HttpRequest get(String host, String path) {
        return HttpRequest.newBuilder(URI.create(host + path)).build();
    }

    protected static HttpRequest patch(String path, String body) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path))
                .method("PATCH", BodyPublishers.ofString(body))
                .build();
    }

    protected static HttpRequest post(String path, String body) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path))
                .header("content-type", "application/json")
                .POST(BodyPublishers.ofString(body))
                .build();
    }

    protected String getHost(Ignite node) {
        int port = cluster.httpPort(cluster.nodeIndex(node.name()));
        return "http://localhost:" + port;
    }

    @Override
    protected int initialNodes() {
        return 0;
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        client = HttpClient.newBuilder().build();

        for (int i = 0; i < 3; i++) {
            cluster.startEmbeddedNode(i);
        }
    }

    protected HttpResponse<String> send(HttpRequest request) {
        try {
            return client.send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
