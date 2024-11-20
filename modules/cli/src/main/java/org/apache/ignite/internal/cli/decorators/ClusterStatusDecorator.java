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

import java.util.List;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStateOutput;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.apache.ignite.rest.client.model.ClusterStateDtoCmgStatus;
import org.apache.ignite.rest.client.model.ClusterStateDtoMetastoreStatus;
import org.apache.ignite.rest.client.model.GroupStatus;

/**
 * Decorator for {@link ClusterStateOutput}.
 */
public class ClusterStatusDecorator implements Decorator<ClusterStateOutput, TerminalOutput> {
    @Override
    public TerminalOutput decorate(ClusterStateOutput data) {
        return data.isInitialized()
                ? () -> toSuccessOutput(data)
                : () -> toNotInitializedOutput(data);
    }

    private static String toSuccessOutput(ClusterStateOutput data) {
        ClusterStateDtoMetastoreStatus metastoreStatus = data.metastoreStatus();
        ClusterStateDtoCmgStatus cmgStatus = data.getCmgStatus();
        return ansi(String.format(
                "[name: %s, nodes: %s, metastoreStatus: %s, cmgStatus: %s]",
                data.getName(),
                data.nodeCount(),
                status(cmgStatus.getAliveNodes(), cmgStatus.getGroupStatus()),
                status(metastoreStatus.getAliveNodes(), metastoreStatus.getGroupStatus())
        ));
    }

    private static String toNotInitializedOutput(ClusterStateOutput data) {
        return ansi(String.format(
                "[nodes: %s, status: %s]",
                data.nodeCount(),
                fg(Color.RED).mark("not initialized")
        ));
    }

    private static String status(List<String> nodes, GroupStatus status) {
        int nodeCount = nodes.size();
        String formattedStatus;
        switch (status) {
            case HEALTHY:
                formattedStatus = fg(Color.GREEN).mark("Healthy");
                break;
            case MAJORITY_LOST:
                formattedStatus = fg(Color.RED).mark("Majority lost");
                break;
            default:
                formattedStatus = "";
        }

        return String.format("[nodes: %s, status: %s]", nodeCount, formattedStatus);
    }
}
