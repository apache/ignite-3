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

package org.apache.ignite.cli.builtins.module;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.IgniteCLIException;

@Singleton
public class ModuleStorage {
    private final CliPathsConfigLoader cliPathsCfgLdr;

    @Inject
    public ModuleStorage(CliPathsConfigLoader cliPathsCfgLdr) {
        this.cliPathsCfgLdr = cliPathsCfgLdr;
    }

    private Path moduleFile() {
        return cliPathsCfgLdr.loadIgnitePathsOrThrowError().installedModulesFile();
    }

    //TODO: write-to-tmp->move approach should be used to prevent file corruption on accidental exit
    public void saveModule(ModuleDefinition moduleDefinition) throws IOException {
        ModuleDefinitionsRegistry moduleDefinitionsRegistry = listInstalled();

        moduleDefinitionsRegistry.modules.add(moduleDefinition);

        ObjectMapper objMapper = new ObjectMapper();

        objMapper.writeValue(moduleFile().toFile(), moduleDefinitionsRegistry);
    }

    //TODO: write-to-tmp->move approach should be used to prevent file corruption on accidental exit
    public boolean removeModule(String name) throws IOException {
        ModuleDefinitionsRegistry moduleDefinitionsRegistry = listInstalled();

        boolean rmv = moduleDefinitionsRegistry.modules.removeIf(m -> m.name.equals(name));

        ObjectMapper objMapper = new ObjectMapper();

        objMapper.writeValue(moduleFile().toFile(), moduleDefinitionsRegistry);

        return rmv;
    }

    public ModuleDefinitionsRegistry listInstalled() {
        var moduleFileAvailable =
            cliPathsCfgLdr.loadIgnitePathsConfig()
                .map(p -> p.installedModulesFile().toFile().exists())
                .orElse(false);

        if (!moduleFileAvailable)
            return new ModuleDefinitionsRegistry(new ArrayList<>());
        else {
            ObjectMapper objMapper = new ObjectMapper();

            try {
                return objMapper.readValue(
                    moduleFile().toFile(),
                    ModuleDefinitionsRegistry.class);
            }
            catch (IOException e) {
                throw new IgniteCLIException("Can't read lsit of installed modules because of IO error", e);
            }
        }
    }

    public static class ModuleDefinitionsRegistry {
        public final List<ModuleDefinition> modules;

        @JsonCreator
        public ModuleDefinitionsRegistry(
            @JsonProperty("modules") List<ModuleDefinition> modules) {
            this.modules = modules;
        }
    }

    public static class ModuleDefinition {
        public final String name;

        public final List<Path> artifacts;

        @Override public String toString() {
            return "ModuleDefinition{" +
                "name='" + name + '\'' +
                ", artifacts=" + artifacts +
                ", cliArtifacts=" + cliArtifacts +
                ", type=" + type +
                ", source='" + source + '\'' +
                '}';
        }

        public final List<Path> cliArtifacts;

        public final SourceType type;

        public final String source;

        @JsonCreator
        public ModuleDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("artifacts") List<Path> artifacts,
            @JsonProperty("cliArtifacts") List<Path> cliArtifacts,
            @JsonProperty("type") SourceType type,
            @JsonProperty("source") String source) {
            this.name = name;
            this.artifacts = artifacts;
            this.cliArtifacts = cliArtifacts;
            this.type = type;
            this.source = source;
        }

        @JsonGetter("artifacts")
        public List<String> artifacts() {
            return artifacts.stream().map(a -> a.toAbsolutePath().toString()).collect(Collectors.toList());
        }

        @JsonGetter("cliArtifacts")
        public List<String> cliArtifacts() {
            return cliArtifacts.stream().map(a -> a.toAbsolutePath().toString()).collect(Collectors.toList());
        }
    }

    public enum SourceType {
        Maven,
        Standard
    }
}
