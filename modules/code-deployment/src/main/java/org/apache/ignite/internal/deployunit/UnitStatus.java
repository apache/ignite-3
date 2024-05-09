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

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.compute.version.Version;

/**
 * Unit meta data class.
 */
public abstract class UnitStatus {
    /**
     * Unit id.
     */
    private final String id;

    /**
     * Unit version.
     */
    private final Version version;

    /**
     * Deployment status.
     */
    private DeploymentStatus status;

    private final UUID opId;

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @param status Unit status.
     * @param opId Deployment unit operation identifier.
     */
    public UnitStatus(String id, Version version, DeploymentStatus status, UUID opId) {
        this.id = id;
        this.version = version;
        this.status = status;
        this.opId = opId;
    }

    /**
     * Returns identifier of deployment unit.
     *
     * @return Identifier of deployment unit.
     */
    public String id() {
        return id;
    }

    /**
     * Returns version of deployment unit.
     *
     * @return Version of deployment unit.
     */
    public Version version() {
        return version;
    }

    /**
     * Returns status of deployment unit.
     *
     * @return Status of deployment unit.
     */
    public DeploymentStatus status() {
        return status;
    }

    /**
     * Updates deployment status.
     *
     * @param status new deployment status.
     */
    public void updateStatus(DeploymentStatus status) {
        this.status = status;
    }

    /**
     * Returns operation identifier of deployment unit creation.
     *
     * @return Operation identifier of deployment unit creation.
     */
    public UUID opId() {
        return opId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnitStatus that = (UnitStatus) o;
        return Objects.equals(id, that.id) && Objects.equals(version, that.version) && status == that.status
                && Objects.equals(opId, that.opId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, status, opId);
    }

    @Override
    public String toString() {
        return "UnitStatus{"
                + "id='" + id + '\''
                + ", version=" + version
                + ", status=" + status
                + ", opId=" + opId
                + '}';
    }
}
