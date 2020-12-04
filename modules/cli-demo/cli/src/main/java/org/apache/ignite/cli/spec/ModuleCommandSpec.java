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

import javax.inject.Inject;
import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.builtins.module.AddModuleCommand;
import org.apache.ignite.cli.common.IgniteCommand;
import org.apache.ignite.cli.builtins.module.ListModuleCommand;
import org.apache.ignite.cli.builtins.module.RemoveModuleCommand;
import picocli.CommandLine;

@CommandLine.Command(name = "module",
    description = "Add or remove Ignite modules and external artifacts.",
    subcommands = {
        ModuleCommandSpec.AddModuleCommandSpec.class,
        ModuleCommandSpec.RemoveModuleCommandSpec.class,
        ModuleCommandSpec.ListModuleCommandSpec.class})
public class ModuleCommandSpec implements IgniteCommand, Runnable {

    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @Override public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @CommandLine.Command(name = "add",
        description = "Add module to Ignite or cli tool")
    public static class AddModuleCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        // TODO: we need to think about the 3d party modules with cli commands
//        @CommandLine.Option(names = {"--cli"},
//            description = "set if you want to install cli module")
//        public boolean cli;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Inject
        ApplicationContext applicationContext;


        @Override public void run() {
            AddModuleCommand addModuleCommand = applicationContext.createBean(AddModuleCommand.class);
            addModuleCommand.setOut(spec.commandLine().getOut());

            addModuleCommand.addModule(moduleName, false);
        }
    }

    @CommandLine.Command(name = "remove",
        description = "Remove Ignite or cli module by name")
    public static class RemoveModuleCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Inject
        ApplicationContext applicationContext;


        @Override public void run() {
            RemoveModuleCommand removeModuleCommand = applicationContext.createBean(RemoveModuleCommand.class);
            removeModuleCommand.setOut(spec.commandLine().getOut());

            removeModuleCommand.removeModule(moduleName);
        }
    }

    @CommandLine.Command(name = "list",
        description = "List available builtin Apache Ignite modules")
    public static class ListModuleCommandSpec implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Inject
        ApplicationContext applicationContext;

        @Override public void run() {
            ListModuleCommand listModuleCommand = applicationContext.createBean(ListModuleCommand.class);
            listModuleCommand.setOut(spec.commandLine().getOut());

            listModuleCommand.list();
        }
    }

}
