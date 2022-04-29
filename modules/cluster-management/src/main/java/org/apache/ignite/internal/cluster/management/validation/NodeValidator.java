/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management.validation;

import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.jetbrains.annotations.Nullable;

/**
 * Class that encapsulates common validation logic.
 */
public class NodeValidator {
    /**
     * Validates a given node (represented by {@code nodeInfo}) against the CMG state.
     *
     * @param cmgState CMG state.
     * @param nodeInfo Node state.
     * @return {@code null} in case of successful validation or a {@link ValidationError} containing some error information.
     */
    @Nullable
    public static ValidationError validateNode(ClusterState cmgState, ValidationInfo nodeInfo) {
        if (!cmgState.igniteVersion().equals(nodeInfo.igniteVersion())) {
            return new ValidationError(String.format(
                    "Ignite versions do not match. Version: %s, version stored in CMG: %s",
                    nodeInfo.igniteVersion(), cmgState.igniteVersion()
            ));
        }

        ClusterTag clusterTag = nodeInfo.clusterTag();

        if (clusterTag != null && !(cmgState.clusterTag().equals(clusterTag))) {
            return new ValidationError(String.format(
                    "Cluster tags do not match. Cluster tag: %s, cluster tag stored in CMG: %s",
                    clusterTag, cmgState.clusterTag()
            ));
        }

        return null;
    }
}
