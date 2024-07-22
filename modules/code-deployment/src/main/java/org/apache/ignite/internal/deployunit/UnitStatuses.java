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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.deployment.version.Version;

/**
 * Deployment unit status.
 */
public class UnitStatuses {
    /**
     * Unit identifier.
     */
    private final String id;

    /**
     * Map from the unit version to the unit status.
     */
    private final List<UnitVersionStatus> versionToStatus;

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param versionToStatus Map from the unit version to the unit status.
     */
    private UnitStatuses(String id, List<UnitVersionStatus> versionToStatus) {
        this.id = id;
        this.versionToStatus = versionToStatus;
        this.versionToStatus.sort(Comparator.comparing(UnitVersionStatus::getVersion));
    }

    /**
     * Returns unit identifier.
     *
     * @return unit identifier.
     */
    public String id() {
        return id;
    }

    public List<UnitVersionStatus> versionStatuses() {
        return Collections.unmodifiableList(versionToStatus);
    }

    /**
     * Builder provider.
     *
     * @param id Identifier of unit. Not null and not blank.
     * @return Instance of {@link UnitStatusesBuilder}.
     */
    public static UnitStatusesBuilder builder(String id) {
        Objects.requireNonNull(id);

        return new UnitStatusesBuilder(id);
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
        UnitStatuses that = (UnitStatuses) o;
        return Objects.equals(id, that.id) && Objects.equals(versionToStatus, that.versionToStatus);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(id, versionToStatus);
    }

    @Override
    public String toString() {
        return "UnitStatuses{"
                + "id='" + id + '\''
                + ", versionToStatus=" + versionToStatus
                + '}';
    }

    /**
     * Builder for {@link UnitStatuses}.
     */
    public static class UnitStatusesBuilder {
        private final String id;

        private final List<UnitVersionStatus> versionToStatus = new CopyOnWriteArrayList<>();

        /**
         * Constructor.
         *
         * @param id unit identifier.
         */
        public UnitStatusesBuilder(String id) {
            this.id = id;
        }

        /**
         * Append unit status with provided version.
         *
         * @param version Unit version.
         * @param deploymentStatus Unit status.
         * @return {@code this} builder for use in a chained invocation.
         */
        public UnitStatusesBuilder append(Version version, DeploymentStatus deploymentStatus) {
            versionToStatus.add(new UnitVersionStatus(version, deploymentStatus));
            return this;
        }

        /**
         * Builder status method.
         *
         * @return {@link UnitStatuses} instance.
         */
        public UnitStatuses build() {
            return new UnitStatuses(id, versionToStatus);
        }
    }
}
