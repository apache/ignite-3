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

package org.apache.ignite.deployment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Data class with deployment unit information.
 */
public class DeploymentInfo {
    private final DeploymentStatus status;

    private final List<String> consistentIds;

    public DeploymentInfo(DeploymentStatus status, List<String> consistentIds) {
        this.status = status;
        this.consistentIds = Collections.unmodifiableList(consistentIds);
    }

    public DeploymentStatus status() {
        return status;
    }

    public List<String> consistentIds() {
        return consistentIds;
    }

    public static DeploymentInfoBuilder builder() {
        return new DeploymentInfoBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DeploymentInfo that = (DeploymentInfo) o;

        return status == that.status
                && (consistentIds != null ? consistentIds.equals(that.consistentIds) : that.consistentIds == null);
    }

    @Override
    public int hashCode() {
        int result = status != null ? status.hashCode() : 0;
        result = 31 * result + (consistentIds != null ? consistentIds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DeploymentInfo{"
                + "status=" + status
                + ", consistentIds=[" + String.join(", ", consistentIds) + "]"
                + '}';
    }

    /**
     * Builder for {@link DeploymentInfo}.
     */
    public static final class DeploymentInfoBuilder {
        private DeploymentStatus status;
        private final List<String> consistentIds = new ArrayList<>();

        public DeploymentInfoBuilder status(DeploymentStatus status) {
            this.status = status;
            return this;
        }

        public DeploymentInfoBuilder addConsistentId(String consistentId) {
            consistentIds.add(consistentId);
            return this;
        }

        public DeploymentInfo build() {
            return new DeploymentInfo(status, consistentIds);
        }

    }
}
