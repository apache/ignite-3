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

package org.apache.ignite.internal.cluster.management;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster tag that is used to uniquely identify a cluster.
 *
 * <p>It consists of two parts: human-readable part (cluster name) that is provided by the init command and an auto-generated part
 * (cluster id).
 */
public final class ClusterTag implements Serializable {
    /** Auto-generated part. */
    private final UUID clusterId = UUID.randomUUID();

    /** Human-readable part. */
    private final String clusterName;

    public ClusterTag(String clusterName) {
        this.clusterName = clusterName;
    }

    public UUID clusterId() {
        return clusterId;
    }

    public String clusterName() {
        return clusterName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterTag that = (ClusterTag) o;
        return clusterId.equals(that.clusterId) && clusterName.equals(that.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, clusterName);
    }

    @Override
    public String toString() {
        return "ClusterTag{"
                + "clusterId=" + clusterId
                + ", clusterName='" + clusterName + '\''
                + '}';
    }
}
