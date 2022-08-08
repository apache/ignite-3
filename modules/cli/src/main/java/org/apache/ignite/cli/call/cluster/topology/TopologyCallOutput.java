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

package org.apache.ignite.cli.call.cluster.topology;

import java.util.List;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.rest.client.model.ClusterNode;

/**
 * Output fot the topology calls.
 */
public class TopologyCallOutput implements CallOutput<List<ClusterNode>> {
    private final Throwable error;

    private final List<ClusterNode> topology;

    private TopologyCallOutput(List<ClusterNode> topology) {
        this.topology = topology;
        error = null;
    }

    private TopologyCallOutput(Exception error) {
        this.error = error;
        this.topology = null;
    }

    public static TopologyCallOutput success(List<ClusterNode> topology) {
        return new TopologyCallOutput(topology);
    }

    public static TopologyCallOutput failure(IgniteCliApiException e) {
        return new TopologyCallOutput(e);
    }

    @Override
    public List<ClusterNode> body() {
        return topology;
    }

    @Override
    public boolean hasError() {
        return error != null;
    }

    @Override
    public boolean isEmpty() {
        return topology != null && topology.isEmpty();
    }

    @Override
    public Throwable errorCause() {
        return error;
    }
}
