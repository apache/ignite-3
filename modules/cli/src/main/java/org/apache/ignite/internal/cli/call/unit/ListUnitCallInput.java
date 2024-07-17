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

package org.apache.ignite.internal.cli.call.unit;

import java.util.List;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.rest.client.model.DeploymentStatus;
import org.jetbrains.annotations.Nullable;

/** Input for {@link ListUnitCall}. */
public class ListUnitCallInput implements CallInput {

    private final String unitId;

    private final String version;

    private final List<DeploymentStatus> statuses;

    private final String url;

    private ListUnitCallInput(@Nullable String unitId, @Nullable String version, @Nullable List<DeploymentStatus> statuses, String url) {
        this.unitId = unitId;
        this.version = version;
        this.statuses = statuses;
        this.url = url;
    }

    public static ListUnitCallInputBuilder builder() {
        return new ListUnitCallInputBuilder();
    }

    @Nullable
    public String unitId() {
        return unitId;
    }

    @Nullable
    public String version() {
        return version;
    }

    @Nullable
    public List<DeploymentStatus> statuses() {
        return statuses;
    }

    public String url() {
        return url;
    }

    /** Builder for {@link ListUnitCallInput}. */
    public static class ListUnitCallInputBuilder {
        @Nullable
        private String unitId;

        @Nullable
        private String version;

        @Nullable
        private List<DeploymentStatus> statuses;

        private String url;

        public ListUnitCallInputBuilder unitId(@Nullable String unitId) {
            this.unitId = unitId;
            return this;
        }

        public ListUnitCallInputBuilder version(@Nullable String version) {
            this.version = version;
            return this;
        }

        public ListUnitCallInputBuilder statuses(@Nullable List<DeploymentStatus> statuses) {
            this.statuses = statuses;
            return this;
        }

        public ListUnitCallInputBuilder url(String url) {
            this.url = url;
            return this;
        }

        public ListUnitCallInput build() {
            return new ListUnitCallInput(unitId, version, statuses, url);
        }
    }
}
