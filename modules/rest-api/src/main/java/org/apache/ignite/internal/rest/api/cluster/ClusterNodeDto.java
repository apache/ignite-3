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

package org.apache.ignite.internal.rest.api.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.ignite.network.ClusterNode;

/**
 * REST representation of {@link ClusterNode}.
 */
@Schema(name = "ClusterNode")
public class ClusterNodeDto {
    /** Local id assigned to this node instance. Changes between restarts. */
    private final String id;

    /** Unique name of member in the cluster. Consistent between restarts. */
    private final String name;

    /** Network address of this node. */
    private final NetworkAddressDto address;

    /**
     * Constructor.
     *
     * @param id Local id that changes between restarts.
     * @param name Unique name of a member in a cluster.
     * @param address Node address.
     */
    @JsonCreator
    public ClusterNodeDto(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("address") NetworkAddressDto address) {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    /**
     * Returns this node's local ID.
     *
     * @return Node's local id.
     */
    @JsonGetter("id")
    public String id() {
        return id;
    }

    /**
     * Returns the unique name (consistent id) of this node in a cluster. Doesn't change between restarts.
     *
     * @return Unique name of the member in a cluster.
     */
    @JsonGetter("name")
    public String name() {
        return name;
    }

    /**
     * Returns the network address of this node.
     *
     * @return Network address of this node.
     */
    @JsonGetter("address")
    public NetworkAddressDto address() {
        return address;
    }
}
