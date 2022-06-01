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

package org.apache.ignite.cli.builtins.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.cli.IgniteCliException;

/**
 * Client for Ignite Cluster management API.
 */
@Singleton
public class ClusterApiClient {
    private static final String CLUSTER_INIT_URL = "/management/v1/cluster/init/";

    private final HttpClient httpClient;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public ClusterApiClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * Sends 'cluster init' command to the specified node.
     *
     * @param nodeEndpoint host:port on which REST API is listening
     * @param metaStorageNodeIds consistent IDs of the nodes that will host the Meta Storage Raft group
     * @param cmgNodeIds         consistent IDs of the nodes that will host the Cluster Management Raft Group; if empty,
     *                           {@code metaStorageNodeIds} will be used to host the CMG as well
     * @param clusterName Human-readable name of the cluster
     * @param out                {@link PrintWriter} to which to report about the command outcome
     */
    public void init(
            String nodeEndpoint,
            List<String> metaStorageNodeIds,
            List<String> cmgNodeIds,
            String clusterName,
            PrintWriter out
    ) {
        InitClusterRequest requestPayload = new InitClusterRequest(metaStorageNodeIds, cmgNodeIds, clusterName);
        String requestJson = toJson(requestPayload);

        var httpRequest = HttpRequest
                .newBuilder()
                .uri(URI.create("http://" + nodeEndpoint + CLUSTER_INIT_URL))
                .method("POST", BodyPublishers.ofString(requestJson))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> httpResponse;
        try {
            httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new IgniteCliException("Connection issues while trying to send http request", e);
        }

        if (httpResponse.statusCode() == HttpURLConnection.HTTP_OK) {
            out.println("Cluster was initialized successfully.");
        } else {
            throw error("Failed to initialize cluster", httpResponse);
        }
    }

    private String toJson(InitClusterRequest requestPayload) {
        try {
            return objectMapper.writeValueAsString(requestPayload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize JSON", e);
        }
    }

    private IgniteCliException error(String message, HttpResponse<String> httpResponse) {
        String errorPayload;
        try {
            errorPayload = prettifyJson(httpResponse);
        } catch (JsonProcessingException e) {
            // not a valid JSON probably
            errorPayload = httpResponse.body();
        }

        return new IgniteCliException(message + System.lineSeparator().repeat(2) + errorPayload);
    }

    private String prettifyJson(HttpResponse<String> httpResponse) throws JsonProcessingException {
        return objectMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(objectMapper.readValue(httpResponse.body(), JsonNode.class));
    }
}
