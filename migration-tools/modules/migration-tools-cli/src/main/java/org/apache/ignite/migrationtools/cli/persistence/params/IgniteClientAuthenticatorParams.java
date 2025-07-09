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

package org.apache.ignite.migrationtools.cli.persistence.params;

import org.apache.ignite3.client.BasicAuthenticator;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

/** IgniteClient Authenticator parameters. */
public class IgniteClientAuthenticatorParams {
    @Nullable
    @CommandLine.Option(names = {"--client.basicAuthenticator.username"}, description = "Username for authenticating the client."
            + " The password should be defined using the 'IGNITE_CLIENT_SECRET' environment variable.")
    private String username;

    /**
     * Retrieves the authenticator instance for these parameters.
     *
     * @return The Basic Authenticator instance.
     */
    @Nullable
    public BasicAuthenticator authenticator() {
        if (username == null) {
            return null;
        }

        String password = System.getenv("IGNITE_CLIENT_SECRET");
        if (password == null) {
            throw new IllegalArgumentException("'IGNITE_CLIENT_SECRET' environment variable was not defined.");
        }

        return new BasicAuthenticator.Builder()
                .username(username)
                .password(password)
                .build();
    }
}
