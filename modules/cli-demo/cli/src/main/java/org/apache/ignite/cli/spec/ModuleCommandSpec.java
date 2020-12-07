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

import java.net.URL;
import java.util.Arrays;
import javax.inject.Inject;
import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.builtins.module.AddModuleCommand;
import org.apache.ignite.cli.common.IgniteCommand;
import org.apache.ignite.cli.builtins.module.ListModuleCommand;
import org.apache.ignite.cli.builtins.module.RemoveModuleCommand;
import picocli.CommandLine;

@CommandLine.Command(
    name = "module",
    description = "Add or remove optional Ignite modules and external artifacts.",
    subcommands = {
        ModuleCommandSpec.AddModuleCommandSpec.class,
        ModuleCommandSpec.RemoveModuleCommandSpec.class,
        ModuleCommandSpec.ListModuleCommandSpec.class
    }
)
public class ModuleCommandSpec extends AbstractCommandSpec implements IgniteCommand {

    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @Override protected void doRun() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @CommandLine.Command(name = "add", description = "Add an optional Ignite module or an external artifact.")
    public static class AddModuleCommandSpec extends AbstractCommandSpec {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        // TODO: we need to think about the 3d party modules with cli commands
//        @CommandLine.Option(names = {"--cli"},
//            description = "set if you want to install cli module")
//        public boolean cli;

        @CommandLine.Option(names = "--repo",
            description = "Url to custom maven repo")
        public URL[] urls;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Inject
        ApplicationContext applicationContext;


        @Override protected void doRun() {
            AddModuleCommand addModuleCommand = applicationContext.createBean(AddModuleCommand.class);
            addModuleCommand.setOut(spec.commandLine().getOut());

            addModuleCommand.addModule(moduleName, Arrays.asList(urls));
        }
    }

    @CommandLine.Command(name = "remove", description = "Add an optional Ignite module or an external artifact.")
    public static class RemoveModuleCommandSpec extends AbstractCommandSpec {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Inject
        ApplicationContext applicationContext;


        @Override protected void doRun() {
            RemoveModuleCommand removeModuleCommand = applicationContext.createBean(RemoveModuleCommand.class);
            removeModuleCommand.setOut(spec.commandLine().getOut());

            removeModuleCommand.removeModule(moduleName);
        }
    }

    @CommandLine.Command(name = "list", description = "Show the list of available optional Ignite modules.")
    public static class ListModuleCommandSpec extends AbstractCommandSpec {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Inject
        ApplicationContext applicationContext;

        @Override protected void doRun() {
            ListModuleCommand listModuleCommand = applicationContext.createBean(ListModuleCommand.class);
            listModuleCommand.setOut(spec.commandLine().getOut());

            listModuleCommand.list();
        }
    }

}
