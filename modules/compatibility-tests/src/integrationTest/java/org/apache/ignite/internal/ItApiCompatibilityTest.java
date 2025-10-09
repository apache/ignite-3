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

package org.apache.ignite.internal;

import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.IgniteVersions.Version;
import org.apache.ignite.internal.compatibility.api.CompatibilityChecker;
import org.apache.ignite.internal.compatibility.api.CompatibilityInput;
import org.apache.ignite.internal.compatibility.api.CompatibilityInput.Builder;
import org.apache.ignite.internal.properties.IgniteProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

@ParameterizedClass
@MethodSource("org.apache.ignite.internal.CompatibilityTestBase#baseVersions")
class ItApiCompatibilityTest {
    private static final List<String> EXCLUDE = IgniteVersions.INSTANCE.apiExcludes();
    private static final Map<String, List<String>> EXCLUDE_PER_VERSION = getApiExcludePerVersion();

    @Parameter
    private String baseVersion;

    @Test
    void testApiModule() {
        checkModule("ignite-api");
    }

    private void checkModule(String module) {
        CompatibilityChecker.check(createInput(baseVersion, module));
    }

    private static CompatibilityInput createInput(String baseVersion, String module) {
        return new Builder()
                .module(module)
                .oldVersion(baseVersion)
                .newVersion(IgniteProperties.get(IgniteProperties.VERSION))
                .currentVersion(true)
                .exclude(constructExclude(baseVersion))
                .errorOnIncompatibility(true)
                .build();
    }

    private static String constructExclude(String version) {
        return String.join(";", EXCLUDE) + ";" + String.join(";", EXCLUDE_PER_VERSION.get(version));
    }

    private static Map<String, List<String>> getApiExcludePerVersion() {
        return IgniteVersions.INSTANCE.versions().stream()
                .collect(toMap(Version::version, Version::apiExcludes));
    }
}
