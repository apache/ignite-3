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

import static org.apache.ignite.internal.OpenApiMatcher.isCompatibleWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.net.URL;
import java.util.stream.Stream;
import org.apache.ignite.internal.IgniteVersions.Version;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Compares OpenAPI specs from previous versions with the current one.
 */
class OpenApiCompatibilityTest {
    private static Stream<String> versions() {
        return IgniteVersions.INSTANCE.versions().stream().map(Version::version);
    }

    @ParameterizedTest
    @MethodSource("versions")
    void compareCurrentSpecWith(String version) {
        URL baseSpec = getClass().getResource("/versions/" + version + "/openapi.yaml");
        URL currentSpec = getClass().getResource("/openapi.yaml");

        assertThat(baseSpec, notNullValue());
        assertThat(currentSpec, notNullValue());

        OpenAPI baseApi = new OpenAPIV3Parser().read(baseSpec.toString());
        assertThat(baseApi.getInfo().getVersion(), is(version));

        OpenAPI currentApi = new OpenAPIV3Parser().read(currentSpec.toString());
        assertThat(currentApi, isCompatibleWith(baseApi));
    }
}
