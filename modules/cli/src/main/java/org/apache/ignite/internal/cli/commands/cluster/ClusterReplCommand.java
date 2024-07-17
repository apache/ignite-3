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

package org.apache.ignite.internal.cli.commands.cluster;

import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.config.ClusterConfigReplCommand;
import org.apache.ignite.internal.cli.commands.cluster.init.ClusterInitReplCommand;
import org.apache.ignite.internal.cli.commands.cluster.status.ClusterStatusReplCommand;
import org.apache.ignite.internal.cli.commands.cluster.topology.TopologyReplCommand;
import org.apache.ignite.internal.cli.commands.cluster.unit.ClusterUnitReplCommand;
import picocli.CommandLine.Command;

/**
 * Cluster command in REPL mode.
 */
@Command(name = "cluster",
        subcommands = {
                ClusterConfigReplCommand.class,
                ClusterInitReplCommand.class,
                ClusterStatusReplCommand.class,
                TopologyReplCommand.class,
                ClusterUnitReplCommand.class
        },
        description = "Manages an Ignite cluster")
public class ClusterReplCommand extends BaseCommand {
}
