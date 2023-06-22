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

package org.apache.ignite.internal.cli.call.cluster.unit;

import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for {@link DeployUnitCall}. */
public class DeployUnitCallInput implements CallInput {

    private final String id;

    private final String version;

    private final Path path;

    private final List<String> nodes;

    private final String clusterUrl;

    private DeployUnitCallInput(String id, String version, Path path, List<String> nodes, String clusterUrl) {
        this.id = id;
        this.version = version;
        this.path = path;
        this.nodes = nodes;
        this.clusterUrl = clusterUrl;
    }

    public static DeployUnitCallBuilder builder() {
        return new DeployUnitCallBuilder();
    }

    public String id() {
        return id;
    }

    public String version() {
        return version;
    }

    public Path path() {
        return path;
    }

    public List<String> nodes() {
        return nodes;
    }

    public String clusterUrl() {
        return clusterUrl;
    }

    /** Builder for {@link DeployUnitCallInput}. */
    public static class DeployUnitCallBuilder {
        private String id;

        private String version;

        private Path path;

        private List<String> nodes;

        private String clusterUrl;

        public DeployUnitCallBuilder id(String id) {
            this.id = id;
            return this;
        }

        public DeployUnitCallBuilder version(String version) {
            this.version = version;
            return this;
        }

        public DeployUnitCallBuilder path(Path path) {
            this.path = path;
            return this;
        }

        public DeployUnitCallBuilder nodes(List<String> nodes) {
            this.nodes = nodes;
            return this;
        }

        public DeployUnitCallBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public DeployUnitCallInput build() {
            return new DeployUnitCallInput(id, version, path, nodes, clusterUrl);
        }
    }
}
