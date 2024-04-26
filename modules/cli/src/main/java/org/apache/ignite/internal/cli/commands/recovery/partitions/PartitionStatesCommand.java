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

package org.apache.ignite.internal.cli.commands.recovery.partitions;

import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAMES_OPTION_DESC;

import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.recovery.PartitionStatesCall;
import org.apache.ignite.internal.cli.call.recovery.PartitionStatesCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.decorators.TableDecorator;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "partition-states", description = "Returns partition states.")
public class PartitionStatesCommand extends BaseCommand implements Callable<Integer> {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    /** Specific local / global states filters. */
    @ArgGroup(exclusive = true, multiplicity = "1")
    private PartitionStatesArgGroup statesArgs;

    /** IDs of partitions to get states of. */
    @Option(names = RECOVERY_PARTITION_IDS_OPTION, description = RECOVERY_PARTITION_IDS_OPTION_DESC, split = ",")
    private List<Integer> partitionIds;

    /** Names of zones to get partition states of. */
    @Option(names = RECOVERY_ZONE_NAMES_OPTION, description = RECOVERY_ZONE_NAMES_OPTION_DESC, split = ",")
    private List<String> zoneNames;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @Inject
    private PartitionStatesCall call;

    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new TableDecorator(plain))
                .verbose(verbose)
                .build()
                .runPipeline();
    }

    private PartitionStatesCallInput buildCallInput() {
        return PartitionStatesCallInput.builder()
                .local(statesArgs.localGroup() != null && statesArgs.localGroup().local())
                .global(statesArgs.global())
                .nodeNames(statesArgs.localGroup() == null ? List.of() : statesArgs.localGroup().nodeNames())
                .zoneNames(zoneNames)
                .partitionIds(partitionIds)
                .clusterUrl(clusterUrl.getClusterUrl())
                .build();
    }
}
