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

package org.apache.ignite.internal.cli.commands.recovery.cluster.migrate;

import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NEW_CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NEW_CLUSTER_URL_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_OLD_CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_OLD_CLUSTER_URL_OPTION_DESC;

import java.net.URL;
import org.apache.ignite.internal.cli.core.converters.RestEndpointUrlConverter;
import picocli.CommandLine.Option;

/** Arguments for 'recovery cluster migrate' command. */
public class MigrateToClusterMixin {
    /** Cluster endpoint URL option for the old cluster (nodes of this cluster will be migrated to a new cluster). */
    @Option(
            names = RECOVERY_OLD_CLUSTER_URL_OPTION,
            description = RECOVERY_OLD_CLUSTER_URL_OPTION_DESC,
            converter = RestEndpointUrlConverter.class,
            required = true
    )
    private URL oldClusterUrl;

    /** Cluster endpoint URL option for the new cluster (nodes of old cluster will be migrated to this cluster). */
    @Option(
            names = RECOVERY_NEW_CLUSTER_URL_OPTION,
            description = RECOVERY_NEW_CLUSTER_URL_OPTION_DESC,
            converter = RestEndpointUrlConverter.class,
            required = true
    )
    private URL newClusterUrl;

    public String getOldClusterUrl() {
        return oldClusterUrl.toString();
    }

    public String getNewClusterUrl() {
        return newClusterUrl.toString();
    }
}
