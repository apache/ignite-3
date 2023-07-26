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

package org.apache.ignite.internal.cli.util;

import picocli.CommandLine.Model.CommandSpec;

/**
 * Utility class for working with {@link CommandSpec}.
 */
public final class CommandSpecUtil {

    /**
     * Finds command spec by command name.
     *
     * @param topCommandSpec Top command spec.
     * @param commands Command names.
     * @return Command spec.
     * @throws IllegalArgumentException If command not found.
     */
    public static CommandSpec findCommand(CommandSpec topCommandSpec, String... commands) {
        CommandSpec commandSpec = topCommandSpec;
        for (String command : commands) {
            if (commandSpec.subcommands().containsKey(command)) {
                commandSpec = commandSpec.subcommands().get(command).getCommandSpec();
            } else {
                throw new IllegalArgumentException("Command not found: " + command);
            }
        }
        return commandSpec;
    }
}
