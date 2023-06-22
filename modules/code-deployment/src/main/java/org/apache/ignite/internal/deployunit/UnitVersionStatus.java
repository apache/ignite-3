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

/**
 * Unit version and status.
 */
public class UnitVersionStatus {
    private final Version version;

    private final DeploymentStatus status;

    /**
     * Constructor.
     *
     * @param version Unit version.
     * @param status Unit status.
     */
    public UnitVersionStatus(Version version, DeploymentStatus status) {
        this.version = version;
        this.status = status;
    }

    public Version getVersion() {
        return version;
    }

    public DeploymentStatus getStatus() {
        return status;
    }
}
