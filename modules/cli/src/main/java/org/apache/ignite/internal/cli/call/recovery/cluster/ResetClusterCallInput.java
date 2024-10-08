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

package org.apache.ignite.internal.cli.call.recovery.cluster;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.cli.commands.recovery.cluster.reset.ResetClusterMixin;
import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for the {@link ResetClusterCall} call. */
public class ResetClusterCallInput implements CallInput {
    private final String clusterUrl;

    private final List<String> cmgNodeNames;

    /** Cluster url. */
    public String clusterUrl() {
        return clusterUrl;
    }

    /** Returns names of the proposed CMG nodes. */
    public List<String> cmgNodeNames() {
        return cmgNodeNames;
    }

    private ResetClusterCallInput(String clusterUrl, List<String> cmgNodeNames) {
        Objects.requireNonNull(cmgNodeNames);

        this.clusterUrl = clusterUrl;
        this.cmgNodeNames = List.copyOf(cmgNodeNames);
    }

    /** Returns {@link ResetClusterCallInput} with specified arguments. */
    public static ResetClusterCallInput of(ResetClusterMixin statesArgs, String clusterUrl) {
        return builder()
                .cmgNodeNames(statesArgs.cmgNodeNames())
                .clusterUrl(clusterUrl)
                .build();
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link ResetClusterCallInput}.
     */
    private static ResetClusterCallInputBuilder builder() {
        return new ResetClusterCallInputBuilder();
    }

    /** Builder for {@link ResetClusterCallInput}. */
    private static class ResetClusterCallInputBuilder {
        private String clusterUrl;

        private List<String> cmgNodeNames;

        /** Set cluster URL. */
        ResetClusterCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        /** Names of the proposed CMG nodes. */
        ResetClusterCallInputBuilder cmgNodeNames(List<String> cmgNodeNames) {
            this.cmgNodeNames = cmgNodeNames;
            return this;
        }

        /** Build {@link ResetClusterCallInput}. */
        ResetClusterCallInput build() {
            return new ResetClusterCallInput(clusterUrl, cmgNodeNames);
        }
    }
}
