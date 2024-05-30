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

package org.apache.ignite.internal.cli.commands.recovery.restart;

import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_TABLE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_TABLE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION_DESC;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/** Arguments for recovery restart partitions command. */
public class RestartPartitionsMixin {
    @Mixin
    private ClusterUrlMixin clusterUrl;

    @Option(names = RECOVERY_PARTITION_IDS_OPTION, description = RECOVERY_PARTITION_IDS_OPTION_DESC, split = ",")
    private List<Integer> partitionIds;

    @Option(names = RECOVERY_NODE_NAMES_OPTION, description = RECOVERY_NODE_NAMES_OPTION_DESC, split = ",")
    private List<String> nodeNames;

    @Option(names = RECOVERY_ZONE_NAME_OPTION, description = RECOVERY_ZONE_NAME_OPTION_DESC, required = true)
    private String zoneName;

    @Option(names = RECOVERY_TABLE_NAME_OPTION, description = RECOVERY_TABLE_NAME_OPTION_DESC, required = true)
    private String tableName;

    /** Returns name of the zone to restart partitions of. */
    public String zoneName() {
        return zoneName;
    }

    /** Returns name of the table to restart partitions of. */
    public String tableName() {
        return tableName;
    }

    /** Returns IDs of partitions to restart partitions of. Empty/null means "all partitions". */
    @Nullable
    public List<Integer> partitionIds() {
        return partitionIds;
    }

    /** Returns names specifying nodes to restart partitions. Empty/null means "all nodes". */
    @Nullable
    public List<String> nodeNames() {
        return nodeNames;
    }

    /** Returns cluster endpoint URL. */
    public String clusterUrl() {
        return clusterUrl.getClusterUrl();
    }
}
