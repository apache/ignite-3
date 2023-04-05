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

import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for {@link UnitStatusCall}. */
public class UnitStatusCallInput implements CallInput {
    private final String id;
    private final String clusterUrl;

    private UnitStatusCallInput(String id, String clusterUrl) {
        this.id = id;
        this.clusterUrl = clusterUrl;
    }

    public String id() {
        return id;
    }

    public String clusterUrl() {
        return clusterUrl;
    }

    public static UnitStatusCallInputBuilder builder() {
        return new UnitStatusCallInputBuilder();
    }

    /** Builder for {@link UnitStatusCallInput}. */
    public static class UnitStatusCallInputBuilder {
        private String id;
        private String clusterUrl;

        public UnitStatusCallInputBuilder id(String id) {
            this.id = id;
            return this;
        }

        public UnitStatusCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public UnitStatusCallInput build() {
            return new UnitStatusCallInput(id, clusterUrl);
        }
    }
}
