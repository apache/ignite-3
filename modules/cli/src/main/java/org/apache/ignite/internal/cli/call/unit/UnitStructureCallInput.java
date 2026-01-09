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

/** Input for unit structure call. */
public class UnitStructureCallInput implements CallInput {

    private final String unitId;

    private final String version;

    private final String url;

    private UnitStructureCallInput(String unitId, String version, String url) {
        this.unitId = unitId;
        this.version = version;
        this.url = url;
    }

    public static UnitStructureCallInputBuilder builder() {
        return new UnitStructureCallInputBuilder();
    }

    public String unitId() {
        return unitId;
    }

    public String version() {
        return version;
    }

    public String url() {
        return url;
    }

    /** Builder for {@link UnitStructureCallInput}. */
    public static class UnitStructureCallInputBuilder {
        private String unitId;

        private String version;

        private String url;

        public UnitStructureCallInputBuilder unitId(String unitId) {
            this.unitId = unitId;
            return this;
        }

        public UnitStructureCallInputBuilder version(String version) {
            this.version = version;
            return this;
        }

        public UnitStructureCallInputBuilder url(String url) {
            this.url = url;
            return this;
        }

        public UnitStructureCallInput build() {
            return new UnitStructureCallInput(unitId, version, url);
        }
    }
}
