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

package org.apache.ignite.internal.cli.commands.cluster.unit;

import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_NODES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_NODES_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_PATH_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_PATH_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_VERSION_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERSION_OPTION;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitCallInput;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

class UnitDeployOptionsMixin {
    /** Unit id. */
    @Parameters(index = "0")
    private String id;

    /** Unit version. */
    @Option(names = VERSION_OPTION, description = UNIT_VERSION_OPTION_DESC, required = true)
    private String version;

    @Spec
    private CommandSpec spec;

    /** Unit path. */
    private Path path;

    @Option(names = UNIT_PATH_OPTION, description = UNIT_PATH_OPTION_DESC, required = true)
    private void setPath(Path value) {
        if (Files.notExists(value)) {
            throw new ParameterException(spec.commandLine(), "No such file or directory: " + value);
        }
        path = value;
    }

    /** Initial set of nodes. */
    private List<String> nodes;

    @Option(names = UNIT_NODES_OPTION, description = UNIT_NODES_OPTION_DESC, split = ",")
    private void setNodes(List<String> values) {
        if (values.size() > 1) {
            List<NodesAlias> aliases = new ArrayList<>();
            List<String> nodeNames = new ArrayList<>();
            values.forEach(it -> {
                String trimmed = it.trim();
                NodesAlias alias = NodesAlias.parse(trimmed);
                if (alias != null) {
                    aliases.add(alias);
                } else {
                    nodeNames.add(trimmed);
                }
            });
            if (aliases.size() > 1) {
                throw new ParameterException(
                        spec.commandLine(),
                        "Aliases " + aliases + " can not be specified together. Provide single alias, please."
                );
            } else if (aliases.size() == 1) {
                throw new ParameterException(
                        spec.commandLine(),
                        "Alias " + aliases.get(0) + " couldn't be used with explicit nodes names list "
                                + nodeNames
                                + ". Provide either node names list or single alias."
                );
            }
        }
        nodes = values;
    }

    DeployUnitCallInput toDeployUnitCallInput(String url) {
        return DeployUnitCallInput.builder()
                .id(id)
                .version(version)
                .path(path)
                .nodes(nodes)
                .clusterUrl(url)
                .build();
    }
}
