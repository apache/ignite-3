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

package org.apache.ignite.internal.deployunit.metastore.status;

import org.apache.ignite.compute.version.Version;
import org.apache.ignite.compute.version.VersionParseException;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;

/**
 * Deployment unit node status.
 */
public class UnitNodeStatus extends UnitStatus {
    private final String nodeId;

    /**
     * Constructor.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param status Deployment unit status.
     * @param nodeId Node consistent id.
     */
    public UnitNodeStatus(String id, Version version, DeploymentStatus status, String nodeId) {
        super(id, version, status);
        this.nodeId = nodeId;
    }

    public String nodeId() {
        return nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        UnitNodeStatus that = (UnitNodeStatus) o;

        return nodeId != null ? nodeId.equals(that.nodeId) : that.nodeId == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
        return result;
    }

    public static byte[] serialize(UnitNodeStatus status) {
        return SerializeUtils.serialize(status.id(), status.version(), status.status(), status.nodeId);
    }

    /**
     * Deserialize method.
     *
     * @param value Serialized deployment unit node status.
     * @return Deserialized deployment unit node status.
     */
    public static UnitNodeStatus deserialize(byte[] value) {
        if (value == null || value.length == 0) {
            return new UnitNodeStatus(null, null, null, null);
        }

        String[] values = SerializeUtils.deserialize(value);

        String id = values.length > 0 ? SerializeUtils.decode(values[0]) : null;
        Version version;
        try {
            version = values.length > 1 ? Version.parseVersion(SerializeUtils.decode(values[1])) : null;
        } catch (VersionParseException e) {
            version = null;
        }
        DeploymentStatus status;
        try {
            status = values.length > 2 ? DeploymentStatus.valueOf(SerializeUtils.decode(values[2])) : null;
        } catch (IllegalArgumentException e) {
            status = null;
        }
        String nodeId = values.length > 3 ? SerializeUtils.decode(values[3]) : null;

        return new UnitNodeStatus(id, version, status, nodeId);
    }
}
