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
import org.apache.ignite.network.NodeMetadata;

/**
 * REST representation of {@link NodeMetadata}.
 */
@Schema(name = "NodeMetadata")
public class NodeMetadataDto {
    private final String restHost;
    private final int restPort;

    /**
     * Constructor.
     *
     * @param restHost REST host of a node.
     * @param restPort REST port of a node.
     */
    @JsonCreator
    public NodeMetadataDto(@JsonProperty("restHost") String restHost, @JsonProperty("restPort") int restPort) {
        this.restHost = restHost;
        this.restPort = restPort;
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
     * Returns this node's REST port.
     *
     * @return REST port.
     */
    @JsonGetter("restPort")
    public int restPort() {
        return restPort;
    }
}
