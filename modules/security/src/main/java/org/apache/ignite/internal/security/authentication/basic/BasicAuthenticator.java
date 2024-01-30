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

package org.apache.ignite.internal.security.authentication.basic;

import static java.util.function.Function.identity;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.security.authentication.AuthenticationRequest;
import org.apache.ignite.internal.security.authentication.Authenticator;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.apache.ignite.internal.security.authentication.UsernamePasswordRequest;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;

/** Implementation of basic authenticator. */
public class BasicAuthenticator implements Authenticator {
    private final String providerName;

    private final Map<String, BasicUser> users;

    /**
     * Constructor.
     *
     * @param providerName Provider name.
     * @param users List of registered users.
     */
    public BasicAuthenticator(String providerName, List<BasicUser> users) {
        this.providerName = providerName;
        this.users = users.stream().collect(Collectors.toMap(BasicUser::name, identity()));
    }

    @Override
    public UserDetails authenticate(AuthenticationRequest<?, ?> authenticationRequest) {
        if (!(authenticationRequest instanceof UsernamePasswordRequest)) {
            throw new UnsupportedAuthenticationTypeException(
                    "Unsupported authentication type: " + authenticationRequest.getClass().getName()
            );
        }

        Object requestUsername = authenticationRequest.getIdentity();
        Object requestPassword = authenticationRequest.getSecret();

        BasicUser basicUser = users.get(requestUsername);
        if (basicUser != null) {
            if (basicUser.password().equals(requestPassword)) {
                return new UserDetails(basicUser.name(), providerName);
            }
        }

        throw new InvalidCredentialsException("Invalid credentials");
    }
}
