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

package org.apache.ignite.internal.cli.commands.connect;

import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_SHORT;

import picocli.CommandLine.Option;

/**
 * Mixin class for connect command options.
 */
public class ConnectOptions {

    @Option(names = {USERNAME_OPTION, USERNAME_OPTION_SHORT}, description = USERNAME_OPTION_DESC,
            required = true, defaultValue = Option.NULL_VALUE)
    private String username;

    @Option(names = {PASSWORD_OPTION, PASSWORD_OPTION_SHORT}, description = PASSWORD_OPTION_DESC,
            required = true, defaultValue = Option.NULL_VALUE)
    private String password;

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }
}
