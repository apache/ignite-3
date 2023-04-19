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

package org.apache.ignite.internal.rest.api.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * REST representation of {@link org.apache.ignite.network.NodeMetadata}.
 */
@Schema(description = "Node metadata information.")
public class NodeMetadata {
    @Schema(description = "The host exposed to REST API.")
    private final String restHost;

    @Schema(description = "The HTTP port exposed to REST API.")
    private final int httpPort;

    @Schema(description = "The HTTPS port exposed to REST API.")
    private final int httpsPort;

    /**
     * Constructor.
     *
     * @param restHost REST host of a node.
     * @param httpPort HTTP port of a node.
     * @param httpsPort HTTPS port of a node.
     */
    @JsonCreator
    public NodeMetadata(
            @JsonProperty("restHost") String restHost,
            @JsonProperty("httpPort") int httpPort,
            @JsonProperty("httpsPort") int httpsPort) {
        this.restHost = restHost;
        this.httpPort = httpPort;
        this.httpsPort = httpsPort;
    }

    /**
     * Returns this node's REST host.
     *
     * @return REST host.
     */
    @JsonGetter("restHost")
    public String restHost() {
        return restHost;
    }

    /**
     * Returns this node's HTTP port.
     *
     * @return HTTP port.
     */
    @JsonGetter("httpPort")
    public int httpPort() {
        return httpPort;
    }

    /**
     * Returns this node's HTTPS port.
     *
     * @return HTTPS port.
     */
    @JsonGetter("httpsPort")
    public int httpsPort() {
        return httpsPort;
    }
}
