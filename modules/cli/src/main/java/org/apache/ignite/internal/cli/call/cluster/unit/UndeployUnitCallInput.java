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

import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for {@link UndeployUnitCall}. */
public class UndeployUnitCallInput implements CallInput {

    private final String id;

    private final String version;

    private final String clusterUrl;

    private UndeployUnitCallInput(String id, String version, String clusterUrl) {
        this.id = id;
        this.version = version;
        this.clusterUrl = clusterUrl;
    }

    public static UndeployUnitCallBuilder builder() {
        return new UndeployUnitCallBuilder();
    }

    public String id() {
        return id;
    }

    public String version() {
        return version;
    }

    public String clusterUrl() {
        return clusterUrl;
    }

    /** Builder for {@link UndeployUnitCallInput}. */
    public static class UndeployUnitCallBuilder {
        private String id;

        private String version;

        private String clusterUrl;

        public UndeployUnitCallBuilder id(String id) {
            this.id = id;
            return this;
        }

        public UndeployUnitCallBuilder version(String version) {
            this.version = version;
            return this;
        }

        public UndeployUnitCallBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public UndeployUnitCallInput build() {
            return new UndeployUnitCallInput(id, version, clusterUrl);
        }
    }
}
