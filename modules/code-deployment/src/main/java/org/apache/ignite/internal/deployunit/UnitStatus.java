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

import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;

/**
 * Unit meta data class.
 */
public class UnitStatus {
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

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @param status Unit status.
     */
    public UnitStatus(String id, Version version, DeploymentStatus status) {
        this.id = id;
        this.version = version;
        this.status = status;
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

    public DeploymentStatus status() {
        return status;
    }

    public void updateStatus(DeploymentStatus status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnitStatus meta = (UnitStatus) o;

        if (id != null ? !id.equals(meta.id) : meta.id != null) {
            return false;
        }
        if (version != null ? !version.equals(meta.version) : meta.version != null) {
            return false;
        }
        return status == meta.status;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UnitStatus{"
                + "id='" + id + '\''
                + ", version=" + version
                + ", status=" + status
                + '}';
    }
}
