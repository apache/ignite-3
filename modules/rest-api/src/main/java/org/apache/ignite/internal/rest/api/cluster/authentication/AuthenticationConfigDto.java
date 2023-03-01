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

package org.apache.ignite.internal.rest.api.cluster.authentication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import org.apache.ignite.rest.AuthenticationConfig;

/**
 * REST representation of {@link AuthenticationConfig}.
 */
@Schema(name = "AuthenticationConfig")
public class AuthenticationConfigDto {

    @Schema(description = "If Security is enabled.")
    private final boolean enabled;

    @Schema(description = "A list of authentication providers.")
    private final List<AuthenticationProviderConfigDto> providers;

    /** Constructor. */
    @JsonCreator
    public AuthenticationConfigDto(
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("providers") List<AuthenticationProviderConfigDto> providers
    ) {

        if (enabled && (providers == null || providers.isEmpty())) {
            throw new IllegalArgumentException("Providers list must not be empty");
        }

        this.enabled = enabled;
        this.providers = providers;
    }

    @JsonProperty
    public boolean enabled() {
        return enabled;
    }

    @JsonProperty
    public List<AuthenticationProviderConfigDto> providers() {
        return providers;
    }

    @Override
    public String toString() {
        return "AuthConfigDto{"
                + "enabled=" + enabled
                + ", providers=" + providers
                + '}';
    }
}
