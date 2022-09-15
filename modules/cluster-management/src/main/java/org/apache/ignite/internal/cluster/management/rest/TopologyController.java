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

package org.apache.ignite.internal.cluster.management.rest;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.rest.api.cluster.ClusterNodeDto;
import org.apache.ignite.internal.rest.api.cluster.NetworkAddressDto;
import org.apache.ignite.internal.rest.api.cluster.TopologyApi;
import org.apache.ignite.internal.rest.exception.ClusterNotInitializedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.TopologyService;

/**
 * Cluster topology endpoint implementation.
 */
@Controller("/management/v1/cluster/topology")
public class TopologyController implements TopologyApi {
    private final TopologyService topologyService;

    private final ClusterManagementGroupManager cmgManager;

    public TopologyController(TopologyService topologyService, ClusterManagementGroupManager cmgManager) {
        this.topologyService = topologyService;
        this.cmgManager = cmgManager;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNodeDto> physicalTopology() {
        return topologyService.allMembers()
                .stream().map(cn -> new ClusterNodeDto(cn.id(), cn.name(),
                        new NetworkAddressDto(cn.address().host(), cn.address().port(), cn.address().consistentId()))
                ).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNodeDto> logicalTopology() {
        try {
            return cmgManager.clusterState()
                    .thenApply(state -> {
                        if (state == null) {
                            throw new ClusterNotInitializedException();
                        }
                        return cmgManager.logicalTopology();
                    }).get().get().stream()
                    .map(cn -> new ClusterNodeDto(cn.id(), cn.name(),
                            new NetworkAddressDto(cn.address().host(), cn.address().port(), cn.address().consistentId()))
                    ).collect(Collectors.toList());
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof ClusterNotInitializedException) {
                throw (ClusterNotInitializedException) e.getCause();
            }
            throw new IgniteException(e);
        }
    }
}
