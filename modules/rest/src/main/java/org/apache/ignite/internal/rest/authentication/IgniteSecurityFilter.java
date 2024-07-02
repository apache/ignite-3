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

package org.apache.ignite.internal.rest.authentication;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.security.config.SecurityConfiguration;
import io.micronaut.security.filters.AuthenticationFetcher;
import io.micronaut.security.filters.SecurityFilter;
import io.micronaut.security.rules.SecurityRule;
import java.util.Collection;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.reactivestreams.Publisher;

/**
 * Replaces {@link SecurityFilter} to disable authentication if it is disabled in Ignite.
 */
@Replaces(SecurityFilter.class)
@Filter(Filter.MATCH_ALL_PATTERN)
@Requires(property = "micronaut.security.enabled", value = "true", defaultValue = "true")
public class IgniteSecurityFilter implements HttpServerFilter, ResourceHolder {
    private final SecurityFilter securityFilter;

    private IgniteAuthenticationProvider igniteAuthenticationProvider;

    /**
     * Constructor.
     *
     * @param securityRules The list of security rules that will allow or reject the request.
     * @param authenticationFetchers List of {@link AuthenticationFetcher} beans in the context.
     * @param securityConfiguration The security configuration.
     * @param igniteAuthenticationProvider The authentication provider.
     */
    public IgniteSecurityFilter(
            Collection<SecurityRule> securityRules,
            Collection<AuthenticationFetcher> authenticationFetchers,
            SecurityConfiguration securityConfiguration,
            IgniteAuthenticationProvider igniteAuthenticationProvider
    ) {
        this.securityFilter = new SecurityFilter(securityRules, authenticationFetchers, securityConfiguration);
        this.igniteAuthenticationProvider = igniteAuthenticationProvider;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        if (igniteAuthenticationProvider.authenticationEnabled()) {
            return securityFilter.doFilter(request, chain);
        } else {
            return chain.proceed(request);
        }
    }

    @Override
    public int getOrder() {
        return securityFilter.getOrder();
    }

    @Override
    public void cleanResources() {
        igniteAuthenticationProvider = null;
    }
}
