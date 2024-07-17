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
 * REST representation of {@link org.apache.ignite.network.NetworkAddress}.
 */
@Schema(description = "Node network address information.")
public class NetworkAddress {
    /** Host. */
    @Schema(description = "Name of the host node runs on.")
    private final String host;

    /** Port. */
    @Schema(description = "Port the node runs on.")
    private final int port;

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     */
    @JsonCreator
    public NetworkAddress(
            @JsonProperty("host") String host,
            @JsonProperty("port") int port
    ) {
        this.host = host;
        this.port = port;
    }

    /**
     * Returns the host name.
     *
     * @return Host name.
     */
    @JsonGetter("host")
    public String host() {
        return host;
    }

    /**
     * Returns the network port.
     *
     * @return Port.
     */
    @JsonGetter("port")
    public int port() {
        return port;
    }
}
