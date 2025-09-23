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

import java.nio.file.Path;
import java.util.Objects;
import org.apache.ignite.deployment.version.Version;

/**
 * Information about a deployment unit used by a job.
 */
public final class DeploymentUnitInfo {
    private final String name;
    private final Version version;
    private final Path path;

    /**
     * Creates deployment unit info.
     *
     * @param name Name of the deployment unit.
     * @param version Version of the deployment unit.
     * @param path Path to the deployment unit resources.
     */
    public DeploymentUnitInfo(String name, Version version, Path path) {
        this.name = Objects.requireNonNull(name, "name");
        this.version = Objects.requireNonNull(version, "version");
        this.path = Objects.requireNonNull(path, "path");
    }

    /**
     * Returns the name of the deployment unit.
     *
     * @return Name of the deployment unit.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the version of the deployment unit.
     *
     * @return Version of the deployment unit.
     */
    public Version version() {
        return version;
    }

    /**
     * Returns the path to the deployment unit resources.
     *
     * @return Path to the deployment unit resources.
     */
    public Path path() {
        return path;
    }

    @Override
    public String toString() {
        return "DeploymentUnitInfo{"
                + "name='" + name + '\''
                + ", version=" + version
                + ", path=" + path
                + '}';
    }
}
