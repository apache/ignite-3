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

package org.apache.ignite.cli.commands.topology;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.BaseCommand;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that prints ignite cluster topology.
 */
@Command(name = "topology", description = "Prints topology information.")
@Singleton
public class TopologyCommand extends BaseCommand {

    /**
     * Cluster url option.
     */
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(
            names = {"--cluster-url"}, description = "Url to cluster node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    @Spec
    private CommandSpec commandSpec;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        //TODO: https://issues.apache.org/jira/browse/IGNITE-17092
        commandSpec.commandLine().getOut().println("Topology command is not implemented yet.");
        return 0;
    }
}
