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

import com.jakewharton.fliptables.FlipTable;
import java.util.List;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.util.PlainTableRenderer;
import org.apache.ignite.rest.client.model.ClusterNode;

/**
 * Implementation of {@link Decorator} for the list of {@link ClusterNode}.
 */
public class TopologyDecorator implements Decorator<List<ClusterNode>, TerminalOutput> {
    /** List of headers to decorate topology. */
    private static final String[] HEADERS = {"name", "host", "port", "consistent id", "id"};

    private final boolean plain;

    public TopologyDecorator(boolean plain) {
        this.plain = plain;
    }

    /**
     * Transform list of {@link ClusterNode} to {@link TerminalOutput}.
     *
     * @param topology incoming list of {@link ClusterNode}.
     * @return User-friendly interpretation of list of {@link ClusterNode} in {@link TerminalOutput}.
     */
    @Override
    public TerminalOutput decorate(List<ClusterNode> topology) {
        if (plain) {
            return () -> PlainTableRenderer.render(HEADERS, topologyToContent(topology));
        } else {
            return () -> FlipTable.of(HEADERS, topologyToContent(topology));
        }
    }

    private static String[][] topologyToContent(List<ClusterNode> topology) {
        return topology.stream().map(
            node -> new String[]{
                node.getName(),
                node.getAddress().getHost(),
                String.valueOf(node.getAddress().getPort()),
                node.getName(),
                node.getId()
            }
        ).toArray(String[][]::new);
    }
}
