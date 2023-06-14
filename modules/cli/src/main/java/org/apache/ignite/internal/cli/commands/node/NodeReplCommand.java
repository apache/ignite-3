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
import org.apache.ignite.internal.cli.commands.node.config.NodeConfigReplCommand;
import org.apache.ignite.internal.cli.commands.node.metric.NodeMetricReplCommand;
import org.apache.ignite.internal.cli.commands.node.status.NodeStatusReplCommand;
import org.apache.ignite.internal.cli.commands.node.unit.NodeUnitReplCommand;
import org.apache.ignite.internal.cli.commands.node.version.NodeVersionReplCommand;
import picocli.CommandLine.Command;

/** Node command in REPL mode. */
@Command(name = "node",
        subcommands = {
                NodeConfigReplCommand.class,
                NodeStatusReplCommand.class,
                NodeVersionReplCommand.class,
                NodeMetricReplCommand.class,
                NodeUnitReplCommand.class
        },
        description = "Node operations")
public class NodeReplCommand extends BaseCommand {
}
