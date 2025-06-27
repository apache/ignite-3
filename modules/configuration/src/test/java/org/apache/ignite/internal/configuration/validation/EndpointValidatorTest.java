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

package org.apache.ignite.internal.configuration.validation;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.mockito.Mockito.mock;

import org.apache.ignite.configuration.validation.Endpoint;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link Endpoint}. */
@ExtendWith(ConfigurationExtension.class)
class EndpointValidatorTest extends BaseIgniteAbstractTest {
    private static final EndpointValidator VALIDATOR = new EndpointValidator();

    @ParameterizedTest
    @ValueSource(strings = {
            "http://127.0.0.1:8080",
            "http://127.0.0.1",
            "http://host",
            "http://host:8080",
            "http://www.host.com",
            "http://www.host.com:8080"
    })
    void validEndpointSuccess(String endpoint) {
        validate(endpoint);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "127.0.0.1:8080",
            "127.0.0.1",
            "host/v1/metrics",
            "www.host.com",
    })
    void invalidEndpointFails(String endpoint) {
        validate(endpoint, "Endpoint must be a valid URL");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ftp://127.0.0.1:8080",
            "file://www.host.com"
    })
    void endpointWithUnexpectedSchemaFails(String endpoint) {
        validate(endpoint, "Endpoint scheme must be http or https");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://www.host.com/?s=1",
            "https://www.host.com/?s=1"
    })
    void endpointWithQueryStringFails(String endpoint) {
        validate(endpoint, "Endpoint must not have a query string");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://www.host.com/v1/metrics#print",
            "https://127.0.0.1:8080/#test"
    })
    void endpointWithFragmentFails(String endpoint) {
        validate(endpoint, "Endpoint must not have a fragment");
    }

    private static void validate(String newValue) {
        validate(newValue, (String[]) null);
    }

    private static void validate(String newValue, String @Nullable ... errorMessagePrefixes) {
        ValidationContext<String> ctx = mockValidationContext(
                null,
                newValue
        );

        TestValidationUtil.validate(
                VALIDATOR,
                mock(Endpoint.class),
                ctx,
                errorMessagePrefixes
        );
    }
}
