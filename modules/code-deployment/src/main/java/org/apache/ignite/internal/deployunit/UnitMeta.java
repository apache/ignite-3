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
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;

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
     * Unit file names.
     */
    private final List<String> fileNames;

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
     * @param fileNames Unit file names.
     * @param consistentIdLocation Consistent ids of nodes where unit deployed.
     */
    public UnitMeta(String id, Version version, List<String> fileNames, DeploymentStatus status, List<String> consistentIdLocation) {
        this.id = id;
        this.version = version;
        this.fileNames = fileNames;
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
    public List<String> fileNames() {
        return fileNames;
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
        if (fileNames != null ? !fileNames.equals(meta.fileNames) : meta.fileNames != null) {
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
        result = 31 * result + (fileNames != null ? fileNames.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + consistentIdLocation.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "UnitMeta{"
                + "id='" + id + '\''
                + ", version=" + version
                + ", fileNames=" + String.join(", ", fileNames)
                + ", status=" + status
                + ", consistentIdLocation=" + String.join(", ", consistentIdLocation)
                + '}';
    }
}
