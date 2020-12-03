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

package org.apache.ignite.cli.spec;

import java.nio.file.Path;
import java.util.List;
import javax.inject.Inject;
import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.builtins.node.StopNodeCommand;
import org.apache.ignite.cli.builtins.node.ListNodesCommand;
import org.apache.ignite.cli.builtins.node.NodesClasspathCommand;
import org.apache.ignite.cli.builtins.node.StartNodeCommand;
import picocli.CommandLine;

@CommandLine.Command(name = "node",
    description = "Start, stop and manage locally running Ignite nodes.", subcommands = {
        NodeCommandSpec.StartNodeCommandSpec.class,
        NodeCommandSpec.StopNodeCommandSpec.class,
        NodeCommandSpec.NodesClasspathCommandSpec.class,
        NodeCommandSpec.ListNodesCommandSpec.class})
public class NodeCommandSpec implements Runnable {

    public @CommandLine.Spec CommandLine.Model.CommandSpec spec;


    @Override public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @CommandLine.Command(name = "start", description = "Start Ignite node")
    public static class StartNodeCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Inject
        ApplicationContext applicationContext;

        @CommandLine.Parameters(paramLabel = "consistent-id", description = "ConsistentId for new node")
        public String consistentId;

        @CommandLine.Option(names = {"--config"}, required = true,
            description = "path to configuration file")
        public Path configPath;

        @Override public void run() {
            StartNodeCommand startNodeCommand = applicationContext.createBean(StartNodeCommand.class);

            startNodeCommand.setOut(spec.commandLine().getOut());
            startNodeCommand.start(consistentId, configPath);
        }
    }

    @CommandLine.Command(name = "stop", description = "Stop Ignite node by consistentId")
    public static class StopNodeCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Inject
        private ApplicationContext applicationContext;

        @CommandLine.Parameters(arity = "1..*", paramLabel = "consistent-ids",
            description = "consistent ids of nodes to start")
        public List<String> pids;

        @Override public void run() {
            StopNodeCommand stopNodeCommand = applicationContext.createBean(StopNodeCommand.class);
            stopNodeCommand.setOut(spec.commandLine().getOut());
            stopNodeCommand.run(pids);

        }
    }

    @CommandLine.Command(name = "list", description = "List current ignite nodes")
    public static class ListNodesCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Inject
        private ApplicationContext applicationContext;

        @Override public void run() {
            ListNodesCommand listNodesCommand = applicationContext.createBean(ListNodesCommand.class);

            listNodesCommand.setOut(spec.commandLine().getOut());
            listNodesCommand.run();

        }
    }

    @CommandLine.Command(name = "classpath", description = "Show current classpath for new nodes")
    public static class NodesClasspathCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Inject
        private ApplicationContext applicationContext;

        @Override public void run() {
            NodesClasspathCommand classpathCommand = applicationContext.createBean(NodesClasspathCommand.class);

            classpathCommand.setOut(spec.commandLine().getOut());
            classpathCommand.run();

        }
    }

}
