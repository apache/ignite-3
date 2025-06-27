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

import org.apache.ignite.internal.cli.call.node.status.NodeStatus;
import org.apache.ignite.internal.cli.call.node.status.State;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;

/**
 * Decorator for {@link NodeStatus}.
 */
public class NodeStatusDecorator implements Decorator<NodeStatus, TerminalOutput> {
    @Override
    public TerminalOutput decorate(NodeStatus data) {
        Color c = data.state().equals(State.STARTED) ? Color.GREEN : Color.YELLOW;

        return () -> ansi(String.format(
                        "[name: %s, state: %s]",
                        data.name(),
                        fg(c).mark(data.state().name().toLowerCase())
                )
        );
    }
}
