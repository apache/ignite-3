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

package org.apache.ignite.internal.cli.commands.node;

import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.node.config.NodeConfigCommand;
import org.apache.ignite.internal.cli.commands.node.metric.NodeMetricCommand;
import org.apache.ignite.internal.cli.commands.node.status.NodeStatusCommand;
import org.apache.ignite.internal.cli.commands.node.unit.NodeUnitCommand;
import org.apache.ignite.internal.cli.commands.node.version.NodeVersionCommand;
import picocli.CommandLine.Command;

/** Node command. */
@Command(name = "node",
        subcommands = {
                NodeConfigCommand.class,
                NodeStatusCommand.class,
                NodeVersionCommand.class,
                NodeMetricCommand.class,
                NodeUnitCommand.class
        },
        description = "Node operations")
public class NodeCommand extends BaseCommand {
}
