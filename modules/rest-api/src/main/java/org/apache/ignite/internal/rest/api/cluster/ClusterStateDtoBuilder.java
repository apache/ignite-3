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

package org.apache.ignite.internal.rest.api.cluster;

import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Builder for {@link ClusterStateDto}.
 */
public final class ClusterStateDtoBuilder {
    private GroupState cmgStatus;

    private GroupState metastoreStatus;

    private String igniteVersion;

    private ClusterTag clusterTag;

    private @Nullable List<UUID> formerClusterIds;

    private ClusterStateDtoBuilder() {

    }

    public static ClusterStateDtoBuilder newInstance() {
        return new ClusterStateDtoBuilder();
    }

    public ClusterStateDtoBuilder cmgStatus(GroupState cmgStatus) {
        this.cmgStatus = cmgStatus;
        return this;
    }

    public ClusterStateDtoBuilder metastoreStatus(GroupState metastoreStatus) {
        this.metastoreStatus = metastoreStatus;
        return this;
    }

    public ClusterStateDtoBuilder igniteVersion(String igniteVersion) {
        this.igniteVersion = igniteVersion;
        return this;
    }

    public ClusterStateDtoBuilder clusterTag(ClusterTag clusterTag) {
        this.clusterTag = clusterTag;
        return this;
    }

    public ClusterStateDtoBuilder formerClusterIds(@Nullable List<UUID> formerClusterIds) {
        this.formerClusterIds = formerClusterIds;
        return this;
    }

    /**
     * Returns new {@link ClusterStateDto} instance.
     */
    public ClusterStateDto build() {
        return new ClusterStateDto(
                cmgStatus,
                metastoreStatus,
                igniteVersion,
                clusterTag,
                formerClusterIds
        );
    }

}
