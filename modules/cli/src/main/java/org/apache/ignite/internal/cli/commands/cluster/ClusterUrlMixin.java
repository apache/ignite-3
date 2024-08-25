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

import static org.apache.ignite.internal.cli.commands.CommandConstants.CLUSTER_URL_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION_DESC;

import java.net.URL;
import org.apache.ignite.internal.cli.core.converters.UrlConverter;
import picocli.CommandLine.Option;

/**
 * Mixin class for cluster URL option.
 */
public class ClusterUrlMixin {
    /** Cluster endpoint URL option. */
    @Option(
            names = CLUSTER_URL_OPTION,
            description = CLUSTER_URL_OPTION_DESC,
            converter = UrlConverter.class,
            order = CLUSTER_URL_OPTION_ORDER
    )
    private URL clusterUrl;

    public String getClusterUrl() {
        return clusterUrl != null ? clusterUrl.toString() : null;
    }
}
