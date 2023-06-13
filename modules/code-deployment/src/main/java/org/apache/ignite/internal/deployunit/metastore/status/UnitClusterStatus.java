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

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.UnitStatus;

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
     * @param depOpId Operation identifier.
     * @param initialNodesToDeploy Nodes required for initial deploy.
     */
    public UnitClusterStatus(
            String id,
            Version version,
            DeploymentStatus status,
            long depOpId,
            Set<String> initialNodesToDeploy
    ) {
        super(id, version, status, depOpId);
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
                status.depOpId(),
                status.initialNodesToDeploy
        );
    }

    /**
     * Deserialize method.
     *
     * @param value Serialized deployment unit cluster status.
     * @return Deserialized deployment unit cluster status.
     */
    public static UnitClusterStatus deserialize(byte[] value) {
        if (value == null || value.length == 0) {
            return new UnitClusterStatus(null, null, null, 0, Set.of());
        }

        String[] values = SerializeUtils.deserialize(value);

        String id = checkElement(values, 0) ? SerializeUtils.decode(values[0]) : null;
        Version version = checkElement(values, 1) ? Version.parseVersion(SerializeUtils.decode(values[1])) : null;
        DeploymentStatus status = checkElement(values, 2) ? DeploymentStatus.valueOf(SerializeUtils.decode(values[2])) : null;
        long depOpId = checkElement(values, 3) ? Long.parseLong(SerializeUtils.decode(values[3])) : 0;
        Set<String> nodes = checkElement(values, 4) ? SerializeUtils.decodeAsSet(values[4]) : Set.of();


        return new UnitClusterStatus(id, version, status, depOpId, nodes);
    }
}
