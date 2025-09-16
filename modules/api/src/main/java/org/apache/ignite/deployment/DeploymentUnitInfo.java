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

/**
 * Information about a deployment unit used by a job.
 */
public class DeploymentUnitInfo {
    private final String name;
    private final String version;
    private final String path;

    /**
     * Creates deployment unit info.
     *
     * @param name Name of the deployment unit.
     * @param version Version of the deployment unit.
     * @param path Path to the deployment unit resources.
     */
    public DeploymentUnitInfo(String name, String version, String path) {
        this.name = Objects.requireNonNull(name, "name");
        this.version = Objects.requireNonNull(version, "version");
        this.path = Objects.requireNonNull(path, "path");
    }

    /**
     * @return Name of the deployment unit.
     */
    public String name() {
        return name;
    }

    /**
     * @return Version of the deployment unit.
     */
    public String version() {
        return version;
    }

    /**
     * @return Path to the deployment unit resources.
     */
    public String path() {
        return path;
    }
}

