/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management;

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager.REST_ENDPOINT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpHeaderNames;
import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.Map;
import org.apache.ignite.network.NetworkAddress;

/**
 * Helper class for initializing an Ignite cluster via REST.
 */
public class RestClusterInitializer {
    public static NetworkAddress DEFAULT_REST_ADDR = new NetworkAddress("localhost", 10300);

    private final HttpClient httpClient = HttpClient.newHttpClient();

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Sends an init command to the given address.
     */
    public void init(NetworkAddress addr, List<String> metaStorageNodeNames, List<String> cmgNodeNames) throws Exception {
        var body = Map.of(
                "metaStorageNodes", metaStorageNodeNames,
                "cmgNodes", cmgNodeNames
        );

        init(addr, body);
    }

    /**
     * Sends an init command to the given address.
     */
    public void init(NetworkAddress addr, List<String> metaStorageNodeNames) throws Exception {
        var body = Map.of("metaStorageNodes", metaStorageNodeNames);

        init(addr, body);
    }

    private void init(NetworkAddress addr, Map<String, ?> body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(restEndpoint(addr))
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

    private static URI restEndpoint(NetworkAddress addr) throws MalformedURLException, URISyntaxException {
        return new URL("http", addr.host(), addr.port(), REST_ENDPOINT).toURI();
    }
}
