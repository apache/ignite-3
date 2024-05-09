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
import static org.apache.ignite.internal.deployunit.metastore.status.SerializeUtils.decode;
import static org.apache.ignite.internal.deployunit.metastore.status.SerializeUtils.decodeAsSet;
import static org.apache.ignite.internal.deployunit.metastore.status.SerializeUtils.deserializeStatus;
import static org.apache.ignite.internal.deployunit.metastore.status.SerializeUtils.deserializeUuid;
import static org.apache.ignite.internal.deployunit.metastore.status.SerializeUtils.deserializeVersion;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Deployment unit cluster status.
 */
public class UnitClusterStatus extends UnitStatus {
    private final Set<String> initialNodesToDeploy;

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @param status Unit status.
     * @param opId Operation identifier.
     * @param initialNodesToDeploy Nodes required for initial deploy.
     */
    public UnitClusterStatus(
            String id,
            Version version,
            DeploymentStatus status,
            UUID opId,
            Set<String> initialNodesToDeploy
    ) {
        super(id, version, status, opId);
        this.initialNodesToDeploy = Collections.unmodifiableSet(initialNodesToDeploy);
    }

    public Set<String> initialNodesToDeploy() {
        return initialNodesToDeploy;
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

        UnitClusterStatus status = (UnitClusterStatus) o;

        return initialNodesToDeploy != null ? initialNodesToDeploy.equals(status.initialNodesToDeploy)
                : status.initialNodesToDeploy == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (initialNodesToDeploy != null ? initialNodesToDeploy.hashCode() : 0);
        return result;
    }

    /**
     * Serialize unit cluster status to byte array.
     *
     * @param status Unit cluster status instance.
     * @return Serialized unit cluster status as byte array.
     */
    public static byte[] serialize(UnitClusterStatus status) {
        return SerializeUtils.serialize(
                status.id(),
                status.version(),
                status.status(),
                status.opId(),
                status.initialNodesToDeploy
        );
    }

    /**
     * Deserialize method.
     *
     * @param value Serialized deployment unit cluster status.
     * @return Deserialized deployment unit cluster status.
     */
    public static UnitClusterStatus deserialize(byte @Nullable [] value) {
        if (value == null || value.length == 0) {
            return new UnitClusterStatus(null, null, null, null, Set.of());
        }

        String[] values = SerializeUtils.deserialize(value);

        String id = checkElement(values, 0) ? decode(values[0]) : null;
        Version version = deserializeVersion(values, 1);
        DeploymentStatus status = deserializeStatus(values, 2);
        UUID opId = deserializeUuid(values, 3);
        Set<String> nodes = checkElement(values, 4) ? decodeAsSet(values[4]) : Set.of();

        return new UnitClusterStatus(id, version, status, opId, nodes);
    }
}
