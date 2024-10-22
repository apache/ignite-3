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

package org.apache.ignite.internal.cli.decorators;

import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.ansi;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.fg;

import org.apache.ignite.internal.cli.call.cluster.status.ClusterState;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.rest.client.model.ClusterStatus;

/**
 * Decorator for {@link ClusterState}.
 */
public class ClusterStatusDecorator implements Decorator<ClusterState, TerminalOutput> {
    @Override
    public TerminalOutput decorate(ClusterState data) {
        return data.isInitialized()
                ? () -> ansi(String.format(
                "[name: %s, nodes: %s, status: %s, cmgNodes: %s, msNodes: %s]",
                data.getName(),
                data.nodeCount(),
                status(data.clusterStatus()),
                data.getCmgNodes(),
                data.getMsNodes()
        ))
                : () -> ansi(String.format(
                        "[nodes: %s, status: %s]",
                        data.nodeCount(), fg(Color.RED).mark("not initialized")
                ));
    }

    private static String status(ClusterStatus status) {
        switch (status) {
            case MS_MAJORITY_LOST:
                return fg(Color.RED).mark("Metastore majority lost");
            case HEALTHY:
                return fg(Color.GREEN).mark("active");
            case CMG_MAJORITY_LOST:
                return fg(Color.RED).mark("CMG majority lost");
            default:
                return "";
        }
    }
}
