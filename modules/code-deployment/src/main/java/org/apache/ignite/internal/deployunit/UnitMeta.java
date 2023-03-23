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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.deployment.DeploymentStatus;
import org.apache.ignite.deployment.version.Version;

/**
 * Unit meta data class.
 */
public class UnitMeta {
    /**
     * Unit id.
     */
    private final String id;

    /**
     * Unit version.
     */
    private final Version version;

    /**
     * Unit name.
     */
    private final String name;

    /**
     * Deployment status.
     */
    private DeploymentStatus status;

    /**
     * Consistent ids of nodes with.
     */
    private final List<String> consistentIdLocation = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @param name Unit name.
     * @param consistentIdLocation Consistent ids of nodes where unit deployed.
     */
    public UnitMeta(String id, Version version, String name, DeploymentStatus status, List<String> consistentIdLocation) {
        this.id = id;
        this.version = version;
        this.name = name;
        this.status = status;
        this.consistentIdLocation.addAll(consistentIdLocation);
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
     * Returns name of deployment unit.
     *
     * @return name of deployment unit.
     */
    public String name() {
        return name;
    }

    /**
     * Returns list of nodes consistent id where deployment unit deployed.
     *
     * @return List of nodes consistent id where deployment unit deployed.
     */
    public List<String> consistentIdLocation() {
        return consistentIdLocation;
    }

    /**
     * Register node as deployment unit holder.
     *
     * @param id Consistent identifier of node.
     */
    public void addConsistentId(String id) {
        consistentIdLocation.add(id);
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

        UnitMeta meta = (UnitMeta) o;

        if (id != null ? !id.equals(meta.id) : meta.id != null) {
            return false;
        }
        if (version != null ? !version.equals(meta.version) : meta.version != null) {
            return false;
        }
        if (name != null ? !name.equals(meta.name) : meta.name != null) {
            return false;
        }
        if (status != meta.status) {
            return false;
        }
        return consistentIdLocation.equals(meta.consistentIdLocation);
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + consistentIdLocation.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "UnitMeta{"
                + "id='" + id + '\''
                + ", version=" + version
                + ", name='" + name + '\''
                + ", status=" + status
                + ", consistentIdLocation=" + String.join(", ", consistentIdLocation)
                + '}';
    }
}
