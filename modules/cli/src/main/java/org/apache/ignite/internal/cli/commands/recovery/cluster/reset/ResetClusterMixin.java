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

package org.apache.ignite.internal.cli.commands.recovery.cluster.reset;

import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_CMG_NODES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_CMG_NODES_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_METASTORAGE_REPLICATION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_METASTORAGE_REPLICATION_OPTION;

import java.util.List;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/** Arguments for 'recovery cluster reset' command. */
public class ResetClusterMixin {
    @ArgGroup(multiplicity = "1", exclusive = false)
    private Options options;

    /** Returns names of the proposed CMG nodes. */
    public @Nullable List<String> cmgNodeNames() {
        return options.cmgNodeNames;
    }

    /** Returns metastorage replication factor. */
    public @Nullable Integer metastorageReplicationFactor() {
        return options.metastorageReplicationFactor;
    }

    private static class Options {
        @Option(names = RECOVERY_CMG_NODES_OPTION, description = RECOVERY_CMG_NODES_OPTION_DESC, split = ",")
        private List<String> cmgNodeNames;

        @Option(names = RECOVERY_METASTORAGE_REPLICATION_OPTION, description = RECOVERY_METASTORAGE_REPLICATION_DESC)
        private Integer metastorageReplicationFactor;
    }
}
