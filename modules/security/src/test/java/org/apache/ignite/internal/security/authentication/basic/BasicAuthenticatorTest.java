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

import static org.apache.ignite.internal.security.authentication.UserDetailsMatcher.username;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.apache.ignite.internal.security.authentication.AnonymousRequest;
import org.apache.ignite.internal.security.authentication.UsernamePasswordRequest;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.junit.jupiter.api.Test;

class BasicAuthenticatorTest {
    private final BasicAuthenticator authenticator = new BasicAuthenticator(
            "basic",
            List.of(new BasicUser("Admin", "password"),
                    new BasicUser("user", "pwd"))
    );

    @Test
    void authenticate() {
        assertThat(
                authenticator.authenticateAsync(new UsernamePasswordRequest("admin", "password")),
                willBe(username(is("Admin")))
        );
    }

    @Test
    void authenticateUpperCase() {
        assertThat(
                authenticator.authenticateAsync(new UsernamePasswordRequest("USER", "pwd")),
                willBe(username(is("user")))
        );
    }

    @Test
    void authenticateInvalidCredentials() {
        assertThat(
                authenticator.authenticateAsync(new UsernamePasswordRequest("admin", "invalid")),
                willThrow(InvalidCredentialsException.class, "Invalid credentials")
        );
    }

    @Test
    void authenticateUnsupportedAuthenticationType() {
        assertThat(
                authenticator.authenticateAsync(new AnonymousRequest()),
                willThrow(
                        UnsupportedAuthenticationTypeException.class,
                        "Unsupported authentication type: " + AnonymousRequest.class.getName()
                )
        );
    }
}
