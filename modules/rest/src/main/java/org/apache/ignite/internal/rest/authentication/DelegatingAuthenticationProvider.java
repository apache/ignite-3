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

import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.Authenticator;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.apache.ignite.internal.security.authentication.UsernamePasswordRequest;
import org.apache.ignite.security.AuthenticationException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Implementation of {@link AuthenticationProvider}. Creates a list of {@link Authenticator} according to provided
 * {@link AuthenticationConfiguration} and updates them on configuration changes. Delegates {@link AuthenticationRequest} to the list of
 * {@link Authenticator}.
 */
public class DelegatingAuthenticationProvider implements AuthenticationProvider {

    private final AuthenticationManager authenticator;

    public DelegatingAuthenticationProvider(AuthenticationManager authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(HttpRequest<?> httpRequest, AuthenticationRequest<?, ?> authenticationRequest) {
        return Flux.create(emitter -> {
            try {
                UserDetails userDetails = authenticator.authenticate(toIgniteAuthenticationRequest(authenticationRequest));
                emitter.next(AuthenticationResponse.success(userDetails.username()));
                emitter.complete();
            } catch (AuthenticationException ex) {
                emitter.error(AuthenticationResponse.exception(ex.getMessage()));
            }
        }, FluxSink.OverflowStrategy.ERROR);
    }

    private static org.apache.ignite.internal.security.authentication.AuthenticationRequest<?, ?> toIgniteAuthenticationRequest(
            AuthenticationRequest<?, ?> authenticationRequest
    ) {
        if (authenticationRequest instanceof UsernamePasswordCredentials) {
            UsernamePasswordCredentials usernamePasswordCredentials = (UsernamePasswordCredentials) authenticationRequest;
            return new UsernamePasswordRequest(
                    usernamePasswordCredentials.getIdentity(),
                    usernamePasswordCredentials.getPassword());
        } else {
            throw new IllegalArgumentException("Unsupported authentication request type: " + authenticationRequest.getClass());
        }
    }
}
