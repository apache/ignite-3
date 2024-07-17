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
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.apache.ignite.internal.security.authentication.UsernamePasswordRequest;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Implementation of {@link AuthenticationProvider}. Delegates authentication to {@link AuthenticationManager}.
 */
public class IgniteAuthenticationProvider implements AuthenticationProvider, ResourceHolder {
    private AuthenticationManager authenticationManager;

    IgniteAuthenticationProvider(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    boolean authenticationEnabled() {
        return authenticationManager.authenticationEnabled();
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(HttpRequest<?> httpRequest, AuthenticationRequest<?, ?> authenticationRequest) {
        return Flux.create(emitter -> {
            try {
                UserDetails userDetails = authenticationManager.authenticate(toIgniteAuthenticationRequest(authenticationRequest));
                emitter.next(AuthenticationResponse.success(userDetails.username()));
                emitter.complete();
            } catch (InvalidCredentialsException ex) {
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

    @Override
    public void cleanResources() {
        authenticationManager = null;
    }
}
