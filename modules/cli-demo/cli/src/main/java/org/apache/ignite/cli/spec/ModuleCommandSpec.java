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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.apache.ignite.cli.builtins.module.ModuleStorage;
import org.apache.ignite.cli.builtins.module.StandardModuleDefinition;
import org.apache.ignite.cli.common.IgniteCommand;
import picocli.CommandLine;

@CommandLine.Command(
    name = "module",
    description = "Manage optional Ignite modules and external artifacts.",
    subcommands = {
        ModuleCommandSpec.ListModuleCommandSpec.class,
        ModuleCommandSpec.AddModuleCommandSpec.class,
        ModuleCommandSpec.RemoveModuleCommandSpec.class
    }
)
public class ModuleCommandSpec extends CategorySpec implements IgniteCommand {
    @CommandLine.Command(name = "add", description = "Add an optional Ignite module or an external artifact.")
    public static class AddModuleCommandSpec extends CommandSpec {

        @Inject private ModuleManager moduleManager;

        @Inject
        private CliPathsConfigLoader cliPathsConfigLoader;

        @CommandLine.Option(names = "--repo",
            description = "Url to custom maven repo")
        public URL[] urls;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Override public void run() {
            var ignitePaths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();
            moduleManager.setOut(spec.commandLine().getOut());
            moduleManager.addModule(moduleName,
                ignitePaths,
                (urls == null)? Collections.emptyList() : Arrays.asList(urls));
        }
    }

    @CommandLine.Command(name = "remove", description = "Add an optional Ignite module or an external artifact.")
    public static class RemoveModuleCommandSpec extends CommandSpec {

        @Inject private ModuleManager moduleManager;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Override public void run() {
            if (moduleManager.removeModule(moduleName))
                spec.commandLine().getOut().println("Module " + moduleName + " was removed successfully");
            else
                spec.commandLine().getOut().println("Module " + moduleName + " is not found");
        }
    }

    @CommandLine.Command(name = "list", description = "Show the list of available optional Ignite modules.")
    public static class ListModuleCommandSpec extends CommandSpec {

        @Inject private ModuleManager moduleManager;
        @Inject private ModuleStorage moduleStorage;

        @Override public void run() {
            var installedModules = new LinkedHashMap<String, ModuleStorage.ModuleDefinition>();

            for (var m: moduleStorage
                .listInstalled()
                .modules
            ) {
                installedModules.put(m.name, m);
            }

            var builtinModules = moduleManager.builtinModules()
                .stream()
                .filter(m -> !m.name.startsWith(ModuleManager.INTERNAL_MODULE_PREFIX))
                .map(m -> new StandardModuleView(m, installedModules.containsKey(m.name)));

            String table = AsciiTable.getTable(builtinModules.collect(Collectors.toList()), Arrays.asList(
                new Column().header("Name").dataAlign(HorizontalAlign.LEFT).with(m -> m.standardModuleDefinition.name),
                new Column().header("Description").dataAlign(HorizontalAlign.LEFT).with(m -> m.standardModuleDefinition.description),
                new Column().header("Installed").dataAlign(HorizontalAlign.LEFT).with(m -> (m.installed) ? "+":"-")
            ));
            spec.commandLine().getOut().println("Official Ignite modules:");
            spec.commandLine().getOut().println(table);

            var externalInstalledModules = installedModules.values().stream()
                .filter(m -> !(m.type == ModuleStorage.SourceType.Standard))
                .collect(Collectors.toList());
            if (!externalInstalledModules.isEmpty()) {
                String externalModulesTable = AsciiTable.getTable(
                    externalInstalledModules,
                    Arrays.asList(
                        new Column().header("Name").dataAlign(HorizontalAlign.LEFT).with(m -> m.name)
                    ));
                spec.commandLine().getOut().println();
                spec.commandLine().getOut().println("External modules:");
                spec.commandLine().getOut().println(externalModulesTable);
            }
        }

        private static class StandardModuleView {
            public final StandardModuleDefinition standardModuleDefinition;
            public final boolean installed;

            public StandardModuleView(StandardModuleDefinition standardModuleDefinition, boolean installed) {
                this.standardModuleDefinition = standardModuleDefinition;
                this.installed = installed;
            }
        }
    }

}
