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

import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_LOCAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_LOCAL_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_GLOBAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_GLOBAL_OPTION_DESC;

import java.util.List;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/** Arguments for {@link PartitionStatesCommand} and {@link PartitionStatesReplCommand}. */
public class PartitionStatesArgGroup {
    @Option(names = RECOVERY_PARTITION_GLOBAL_OPTION, description = RECOVERY_PARTITION_GLOBAL_OPTION_DESC)
    private boolean global;

    @ArgGroup(exclusive = false)
    private LocalGroup localGroup;

    /** If global partition states should be returned. */
    public boolean global() {
        return global;
    }

    /** Returns arguments specific to local partition states. */
    LocalGroup localGroup() {
        return localGroup;
    }

    /** Arguments specific to local partition states. */
    public static class LocalGroup {
        @Option(required = true, names = RECOVERY_LOCAL_OPTION, description = RECOVERY_LOCAL_OPTION_DESC)
        private boolean local;

        @Option(names = RECOVERY_NODE_NAMES_OPTION, description = RECOVERY_NODE_NAMES_OPTION_DESC, split = ",")
        private List<String> nodeNames;

        /** Returns node names to get partition states from. */
        public List<String> nodeNames() {
            return nodeNames;
        }

        /** If local partition states should be returned. */
        public boolean local() {
            return local;
        }
    }
}
