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

package org.apache.ignite.internal.compatibility.api;

import java.util.List;
import org.apache.ignite.internal.IgniteVersions;
import org.apache.ignite.internal.IgniteVersions.Version;
import org.apache.ignite.internal.properties.IgniteProperties;

public class CompatibilityInput {
    private final String module;
    private final String oldVersion;
    private final String newVersion;
    private final String exclude;
    private final boolean errorOnIncompatibility;
    private final boolean currentVersion;

    private CompatibilityInput(
            String module,
            String oldVersion,
            String newVersion,
            String exclude,
            boolean errorOnIncompatibility,
            boolean currentVersion
    ) {
        this.module = module;
        this.oldVersion = oldVersion;
        this.newVersion = newVersion;
        this.exclude = exclude;
        this.errorOnIncompatibility = errorOnIncompatibility;
        this.currentVersion = currentVersion;
    }

    String module() {
        return module;
    }

    String oldVersionNotation() {
        return "org.apache.ignite:" + module + ":" + oldVersion;
    }

    String newVersionNotation() {
        return "org.apache.ignite:" + module + ":" + newVersion;
    }

    String exclude() {
        return exclude;
    }

    boolean errorOnIncompatibility() {
        return errorOnIncompatibility;
    }

    boolean currentVersion() {
        return currentVersion;
    }

    public static class Builder {
        private String module;
        private String oldVersion;
        private String newVersion;
        private String exclude;
        private boolean errorOnIncompatibility = true;

        public Builder module(String module) {
            this.module = module;
            return this;
        }

        public Builder oldVersion(String oldVersion) {
            this.oldVersion = oldVersion;
            return this;
        }

        public Builder newVersion(String newVersion) {
            this.newVersion = newVersion;
            return this;
        }

        public Builder exclude(String exclude) {
            this.exclude = exclude;
            return this;
        }

        public Builder errorOnIncompatibility(boolean errorOnIncompatibility) {
            this.errorOnIncompatibility = errorOnIncompatibility;
            return this;
        }

        public CompatibilityInput build() {
            return new CompatibilityInput(
                    module,
                    oldVersion,
                    newVersion != null ? newVersion : IgniteProperties.get(IgniteProperties.VERSION),
                    exclude != null ? exclude : constructExclude(oldVersion),
                    errorOnIncompatibility,
                    newVersion == null
            );
        }

        private static String constructExclude(String version) {
            List<String> exclude = IgniteVersions.INSTANCE.getOrDefault(version, Version::apiExcludes, IgniteVersions::apiExcludes);
            return exclude != null ? String.join(";", exclude) : "";
        }
    }
}
