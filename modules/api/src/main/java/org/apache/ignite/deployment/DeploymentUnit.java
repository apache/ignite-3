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

package org.apache.ignite.deployment;

import java.util.Objects;
import org.apache.ignite.deployment.version.Version;

/**
 * Deployment unit.
 */
public class DeploymentUnit {

    /** Name of the deployment unit. */
    private final String name;

    /** Version of the deployment unit. */
    private final Version version;

    /**
     * Constructor.
     *
     * @param name Name of the deployment unit.
     * @param version Version of the deployment unit.
     */
    public DeploymentUnit(String name, Version version) {
        this.name = name;
        this.version = version;
    }

    /**
     * Constructor.
     *
     * @param name Name of the deployment unit.
     * @param version Version of the deployment unit.
     */
    public DeploymentUnit(String name, String version) {
        this(name, Version.parseVersion(version));
    }

    /**
     * Returns name of the deployment unit.
     *
     * @return Name of the deployment unit.
     */
    public String name() {
        return name;
    }

    /**
     * Returns version of the deployment unit.
     *
     * @return Version of the deployment unit.
     */
    public Version version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeploymentUnit unit = (DeploymentUnit) o;
        return Objects.equals(name, unit.name) && Objects.equals(version, unit.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version);
    }

    @Override
    public String toString() {
        return "DeploymentUnit{"
                + "name='" + name + '\''
                + ", version=" + version
                + '}';
    }

    public String render() {
        return name + ":" + version;
    }
}
