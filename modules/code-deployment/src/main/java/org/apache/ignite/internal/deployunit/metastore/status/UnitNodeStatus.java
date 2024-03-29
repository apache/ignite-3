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

import static org.apache.ignite.internal.deployunit.metastore.status.SerializeUtils.checkElement;

import org.apache.ignite.compute.version.Version;
import org.apache.ignite.compute.version.VersionParseException;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.jetbrains.annotations.Nullable;

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
     * @param opId Deployment unit operation identifier.
     * @param nodeId Node consistent id.
     */
    public UnitNodeStatus(String id, Version version, DeploymentStatus status, long opId, String nodeId) {
        super(id, version, status, opId);
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

    /**
     * Serialize unit node status to byte array.
     *
     * @param status Unit node status instance.
     * @return Serialized unit node status as byte array.
     */
    public static byte[] serialize(UnitNodeStatus status) {
        return SerializeUtils.serialize(
                status.id(),
                status.version(),
                status.status(),
                status.opId(),
                status.nodeId
        );
    }

    /**
     * Deserialize method.
     *
     * @param value Serialized deployment unit node status.
     * @return Deserialized deployment unit node status.
     */
    public static UnitNodeStatus deserialize(byte @Nullable [] value) {
        if (value == null || value.length == 0) {
            return new UnitNodeStatus(null, null, null, -1, null);
        }

        String[] values = SerializeUtils.deserialize(value);

        String id = checkElement(values, 0) ? SerializeUtils.decode(values[0]) : null;
        Version version;
        try {
            version = checkElement(values, 1) ? Version.parseVersion(SerializeUtils.decode(values[1])) : null;
        } catch (VersionParseException e) {
            version = null;
        }
        DeploymentStatus status;
        try {
            status = checkElement(values, 2) ? DeploymentStatus.valueOf(SerializeUtils.decode(values[2])) : null;
        } catch (IllegalArgumentException e) {
            status = null;
        }

        long opId = checkElement(values, 3) ? Long.parseLong(SerializeUtils.decode(values[3])) : 0;

        String nodeId = checkElement(values, 4) ? SerializeUtils.decode(values[4]) : null;

        return new UnitNodeStatus(id, version, status, opId, nodeId);
    }
}
