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

package org.apache.ignite.internal.cli.commands.cluster.init;

import static org.apache.ignite.internal.cli.commands.Options.Constants.AUTHENTICATION_ENABLED_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.AUTHENTICATION_ENABLED_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.AUTHENTICATION_ENABLED_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.BASIC_AUTHENTICATION_LOGIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.BASIC_AUTHENTICATION_LOGIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.BASIC_AUTHENTICATION_LOGIN_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.BASIC_AUTHENTICATION_PASSWORD_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.BASIC_AUTHENTICATION_PASSWORD_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.BASIC_AUTHENTICATION_PASSWORD_OPTION_SHORT;

import picocli.CommandLine.Option;

/**
 * Mixin class for authentication options.
 */
public class AuthenticationOptions {

    @Option(names = {AUTHENTICATION_ENABLED_OPTION, AUTHENTICATION_ENABLED_OPTION_SHORT},
            description = AUTHENTICATION_ENABLED_OPTION_DESC)
    private boolean enabled;

    @Option(names = {BASIC_AUTHENTICATION_LOGIN_OPTION, BASIC_AUTHENTICATION_LOGIN_OPTION_SHORT},
            description = BASIC_AUTHENTICATION_LOGIN_OPTION_DESC)
    private String basicLogin;

    @Option(names = {BASIC_AUTHENTICATION_PASSWORD_OPTION, BASIC_AUTHENTICATION_PASSWORD_OPTION_SHORT},
            description = BASIC_AUTHENTICATION_PASSWORD_OPTION_DESC)
    private String basicPassword;

    public boolean enabled() {
        return enabled;
    }

    public String basicLogin() {
        return basicLogin;
    }

    public String basicPassword() {
        return basicPassword;
    }
}
