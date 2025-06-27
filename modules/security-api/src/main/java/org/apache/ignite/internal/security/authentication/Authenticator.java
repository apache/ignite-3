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

package org.apache.ignite.internal.security.authentication;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;

/**
 * General interface for all authenticators.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface Authenticator {
    /**
     * Authenticates a user with the given request. Returns the user details if the authentication was successful. Throws an exception
     * otherwise.
     *
     * <p>Implementations should be non-blocking, this method can be called from an IO thread.
     *
     * @param authenticationRequest The authentication request.
     * @return The user details.
     * @throws InvalidCredentialsException If the authentication failed.
     * @throws UnsupportedAuthenticationTypeException If the authentication type is not supported.
     */
    CompletableFuture<UserDetails> authenticateAsync(AuthenticationRequest<?, ?> authenticationRequest);
}
