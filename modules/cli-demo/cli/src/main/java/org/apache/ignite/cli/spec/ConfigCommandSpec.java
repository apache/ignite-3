/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.spec;

import org.apache.ignite.cli.builtins.config.ConfigurationClient;
import picocli.CommandLine;

@CommandLine.Command(name = "config", mixinStandardHelpOptions = true,
    description = "Inspect and update Ignite cluster configuration.",
    subcommands = {
        ConfigCommandSpec.GetConfigCommandSpec.class,
        ConfigCommandSpec.SetConfigCommandSpec.class
    })
public class ConfigCommandSpec implements Runnable {

    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @Override public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @CommandLine.Command(name = "get", mixinStandardHelpOptions = true,
        description = "Get current cluster configs")
    public static class GetConfigCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Override public void run() {
            spec.commandLine().getOut().println(new ConfigurationClient().get());
        }
    }

    @CommandLine.Command(name = "set", mixinStandardHelpOptions = true,
        description = "Set current cluster configs. Config is expected as any valid Hocon")
    public static class SetConfigCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @CommandLine.Parameters(paramLabel = "hocon-string", description = "any text representation of hocon config")
        private String config;

        @Override public void run() {
            spec.commandLine().getOut().println(new ConfigurationClient().set(config));
        }
    }
}
