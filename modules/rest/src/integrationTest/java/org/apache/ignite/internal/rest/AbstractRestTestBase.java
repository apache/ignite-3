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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.rest.api.Problem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for integration REST test.
 */
public abstract class AbstractRestTestBase extends ClusterPerTestIntegrationTest {
    /** HTTP host and port url part. */
    private static final String HTTP_HOST_PORT = "http://localhost:10300";

    protected ObjectMapper objectMapper;

    /** HTTP client. */
    private HttpClient client;

    protected static HttpRequest get(String path) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path)).build();
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

    @Override
    protected int initialNodes() {
        return 0;
    }

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException, InterruptedException {
        objectMapper = new ObjectMapper();
        client = HttpClient.newBuilder().build();

        for (int i = 0; i < 3; i++) {
            cluster.startEmbeddedNode(i);
        }
    }

    protected HttpResponse<String> send(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, BodyHandlers.ofString());
    }

    protected Problem getProblem(HttpResponse<String> initResponse) throws JsonProcessingException {
        return objectMapper.readValue(initResponse.body(), Problem.class);
    }
}
