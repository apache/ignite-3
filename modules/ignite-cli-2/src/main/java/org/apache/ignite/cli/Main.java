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

package org.apache.ignite.cli;

import io.micronaut.configuration.picocli.MicronautFactory;
import java.util.HashMap;
import org.apache.ignite.cli.commands.CliCommands;
import org.apache.ignite.cli.core.CliManager;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;

/**
 * Ignite cli entry point.
 */
public class Main {
    private static final String name = "ignite-cli";

    /**
     * Entry point.
     *
     * @param args ignore.
     */
    public static void main(String[] args) throws Exception {
        MicronautFactory micronautFactory = new MicronautFactory();
        CliManager cliManager = micronautFactory.create(CliManager.class);
        cliManager.init(micronautFactory);
        HashMap<String, String> aliases = new HashMap<>();
        aliases.put("zle", "widget");
        aliases.put("bindkey", "keymap");
        cliManager.enableRepl(Repl.builder()
                .withName(name)
                .withCommandsClass(CliCommands.class)
                .withCommandExecutorProvider(RegistryCommandExecutor::new)
                .withAliases(aliases)
                .build());
    }

}
