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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.ignite.internal.security.authentication.AnonymousRequest;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.apache.ignite.internal.security.authentication.UsernamePasswordRequest;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.junit.jupiter.api.Test;

class BasicAuthenticatorTest {
    private final BasicAuthenticator authenticator = new BasicAuthenticator(
            "basic",
            List.of(new BasicUser("admin", "password"))
    );

    @Test
    void authenticate() {
        // when
        UserDetails userDetails = authenticator.authenticateAsync(new UsernamePasswordRequest("admin", "password")).join();

        // then
        assertEquals("admin", userDetails.username());
    }

    @Test
    void authenticateInvalidCredentials() {
        // when
        InvalidCredentialsException exception = assertThrows(InvalidCredentialsException.class, () -> {
            authenticator.authenticateAsync(new UsernamePasswordRequest("admin", "invalid"));
        });

        // then
        assertEquals("Invalid credentials", exception.getMessage());
    }

    @Test
    void authenticateUnsupportedAuthenticationType() {
        // when
        UnsupportedAuthenticationTypeException exception = assertThrows(
                UnsupportedAuthenticationTypeException.class,
                () -> authenticator.authenticateAsync(new AnonymousRequest())
        );

        // then
        assertEquals("Unsupported authentication type: " + AnonymousRequest.class.getName(), exception.getMessage());
    }
}
