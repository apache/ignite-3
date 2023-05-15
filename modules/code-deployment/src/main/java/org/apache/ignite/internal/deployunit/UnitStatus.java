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

package org.apache.ignite.internal.deployunit;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.deployunit.version.Version;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;

/**
 * Deployment unit status.
 */
public class UnitStatus {
    /**
     * Unit identifier.
     */
    private final String id;

    /**
     * Map from existing unit version to list of nodes consistent ids where unit deployed.
     */
    private final Map<Version, DeploymentStatus> versionToStatus;

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param versionToConsistentIds Map from existing unit version to list
     *      of nodes consistent ids where unit deployed.
     */
    private UnitStatus(String id,
            Map<Version, DeploymentStatus> versionToConsistentIds) {
        this.id = id;
        this.versionToStatus = Collections.unmodifiableMap(versionToConsistentIds);
    }

    /**
     * Returns unit identifier.
     *
     * @return unit identifier.
     */
    public String id() {
        return id;
    }

    /**
     * Returns unit version.
     *
     * @return unit version.
     */
    public Set<Version> versions() {
        return Collections.unmodifiableSet(versionToStatus.keySet());
    }

    public DeploymentStatus status(Version version) {
        return versionToStatus.get(version);
    }

    /**
     * Builder provider.
     *
     * @param id Identifier of unit. Not null and not blank.
     * @return Instance of {@link UnitStatusBuilder}.
     */
    public static UnitStatusBuilder builder(String id) {
        Objects.requireNonNull(id);

        return new UnitStatusBuilder(id);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnitStatus that = (UnitStatus) o;
        return Objects.equals(id, that.id) && Objects.equals(versionToStatus, that.versionToStatus);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(id, versionToStatus);
    }

    @Override
    public String toString() {
        return "UnitStatus{"
                + "id='" + id + '\''
                + ", versionToStatus=" + versionToStatus
                + '}';
    }

    /**
     * Builder for {@link UnitStatus}.
     */
    public static class UnitStatusBuilder {

        private final String id;
        private final Map<Version, DeploymentStatus> versionToStatus = new ConcurrentHashMap<>();

        /**
         * Constructor.
         *
         * @param id unit identifier.
         */
        public UnitStatusBuilder(String id) {
            this.id = id;
        }

        /**
         * Append node consistent ids with provided version.
         *
         * @param version Unit version.
         * @param deploymentInfo Node consistent ids.
         * @return {@code this} builder for use in a chained invocation.
         */
        public UnitStatusBuilder append(Version version, DeploymentStatus deploymentInfo) {
            versionToStatus.put(version, deploymentInfo);
            return this;
        }

        /**
         * Builder status method.
         *
         * @return {@link UnitStatus} instance.
         */
        public UnitStatus build() {
            return new UnitStatus(id, versionToStatus);
        }
    }
}

